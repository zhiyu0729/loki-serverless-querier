package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const requestIDHeader = "Lambda-Runtime-Aws-Request-Id"
const deadlineHeader = "Lambda-Runtime-Deadline-Ms"

type Handler func(context.Context, []byte) ([]byte, error)

func Serve(ctx context.Context, handler Handler, clientTimeout time.Duration) error {
	runtimeAPI := os.Getenv("AWS_LAMBDA_RUNTIME_API")
	if runtimeAPI == "" {
		return errors.New("AWS_LAMBDA_RUNTIME_API is not set")
	}
	if clientTimeout <= 0 {
		clientTimeout = 0
	}

	client := &http.Client{Timeout: clientTimeout}
	baseURL := "http://" + runtimeAPI + "/2018-06-01/runtime/invocation"

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/next", nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		payload, readErr := io.ReadAll(resp.Body)
		closeErr := resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		if resp.StatusCode/100 != 2 {
			return fmt.Errorf("lambda runtime next returned %s: %s", resp.Status, string(payload))
		}

		requestID := resp.Header.Get(requestIDHeader)
		if requestID == "" {
			return errors.New("lambda runtime response missing request id")
		}

		invokeCtx := contextWithDeadline(ctx, resp.Header.Get(deadlineHeader))
		out, err := handler(invokeCtx, payload)
		if err != nil {
			if postErr := postError(ctx, client, baseURL, requestID, err); postErr != nil {
				return postErr
			}
			continue
		}
		if err := postResponse(ctx, client, baseURL, requestID, out); err != nil {
			return err
		}
	}
}

func contextWithDeadline(ctx context.Context, deadlineMS string) context.Context {
	if deadlineMS == "" {
		return ctx
	}
	ms, err := strconv.ParseInt(deadlineMS, 10, 64)
	if err != nil {
		return ctx
	}
	deadline := time.UnixMilli(ms)
	child, _ := context.WithDeadline(ctx, deadline)
	return child
}

func postResponse(ctx context.Context, client *http.Client, baseURL, requestID string, body []byte) error {
	url := fmt.Sprintf("%s/%s/response", baseURL, requestID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("lambda runtime response post returned %s: %s", resp.Status, string(raw))
	}
	return nil
}

func postError(ctx context.Context, client *http.Client, baseURL, requestID string, invokeErr error) error {
	body, _ := json.Marshal(map[string]string{
		"errorType":    "HandlerError",
		"errorMessage": invokeErr.Error(),
	})
	url := fmt.Sprintf("%s/%s/error", baseURL, requestID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("lambda runtime error post returned %s: %s", resp.Status, string(raw))
	}
	return nil
}
