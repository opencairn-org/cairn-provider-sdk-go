package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/opencairn-org/cairn-provider-sdk-go/port"
	"github.com/opencairn-org/cairn-provider-sdk-go/webhook"
	"github.com/opencairn-org/cairn-provider-sdk-go/workersdk"
)

// acmeEvent is Acme's webhook payload; the signature header is
// "X-Acme-Signature: sha256=<hex HMAC of the body>".
type acmeEvent struct {
	OwnerID    string `json:"owner_id"`
	ActivityID string `json:"activity_id"`
	Type       string `json:"type"` // "created" | "updated" | "deleted"
}

// makeWebhookEvent verifies the signature, resolves the owner to a Cairn
// account and enqueues a fetch. Unknown owners are dropped (ACK): the
// account was disconnected, reconcile covers drift.
func makeWebhookEvent(secret []byte) workersdk.WebhookEventHandler {
	return func(ctx context.Context, w *workersdk.Worker, ev workersdk.WebhookEvent) error {
		env, err := ev.Envelope()
		if err != nil {
			return err
		}
		if !webhook.Verify(secret, env.Body, env.Headers["x-acme-signature"], webhook.Hex) {
			return &port.TerminalError{Reason: "bad_signature"}
		}
		var e acmeEvent
		if err := json.Unmarshal(env.Body, &e); err != nil {
			return &port.TerminalError{Reason: "bad_event_payload", Cause: err}
		}
		accountID, userID, err := lookupAccount(ctx, w, e.OwnerID)
		if err != nil {
			w.Logger().Info("webhook for unknown owner; dropping", "owner_id", e.OwnerID, "error", err)
			return nil
		}
		switch e.Type {
		case "created", "updated":
			return w.EnqueueFetchSource(ctx, "webhook", accountID, userID, "webhook", e.ActivityID)
		case "deleted":
			body, _ := json.Marshal(map[string]string{"account_id": accountID, "user_id": userID, "provider": workerProvider, "external_id": e.ActivityID})
			return w.PublishEvent(ctx, "source.deleted_upstream", "deleted:"+workerProvider+":"+e.ActivityID, body)
		default:
			return nil
		}
	}
}

// lookupAccount asks core which connection owns the provider account.
func lookupAccount(ctx context.Context, w *workersdk.Worker, ownerID string) (accountID, userID string, err error) {
	req, _ := json.Marshal(map[string]string{"provider": workerProvider, "external_id": ownerID, "worker_name": w.Name()})
	resp, err := w.Request(ctx, w.LookupSubject(), req, time.Second)
	if err != nil {
		return "", "", err
	}
	var out struct {
		AccountID string `json:"account_id"`
		UserID    string `json:"user_id"`
		Error     string `json:"error"`
	}
	if err := json.Unmarshal(resp, &out); err != nil {
		return "", "", err
	}
	if out.Error != "" {
		return "", "", errors.New(out.Error)
	}
	return out.AccountID, out.UserID, nil
}

// makeWebhookVerify answers Acme's subscription handshake: echo the
// challenge when the verify token matches.
func makeWebhookVerify(secret string) workersdk.WebhookVerifyHandler {
	return func(_ context.Context, _ *workersdk.Worker, req workersdk.WebhookVerifyRequest) (workersdk.WebhookVerifyResponse, error) {
		if req.Query["verify_token"] != secret {
			return workersdk.WebhookVerifyResponse{Status: http.StatusForbidden, ContentType: "text/plain", Body: []byte("invalid verify_token")}, nil
		}
		return workersdk.WebhookVerifyResponse{Status: http.StatusOK, ContentType: "text/plain", Body: []byte(req.Query["challenge"])}, nil
	}
}
