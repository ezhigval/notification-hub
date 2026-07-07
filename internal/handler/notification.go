package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/ezhigval/go-toolkit/httputil"
	"github.com/ezhigval/notification-hub/internal/model"
	"github.com/ezhigval/notification-hub/internal/service"
	"github.com/go-chi/chi/v5"
)

type Handler struct {
	svc *service.NotificationService
}

func New(svc *service.NotificationService) *Handler {
	return &Handler{svc: svc}
}

func (h *Handler) CreateTemplate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name            string        `json:"name"`
		Channel         model.Channel `json:"channel"`
		SubjectTemplate string        `json:"subject_template"`
		BodyTemplate    string        `json:"body_template"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteError(w, httputil.NewAppError(http.StatusBadRequest, "BAD_REQUEST", "invalid json", err))
		return
	}
	t, err := h.svc.CreateTemplate(r.Context(), req.Name, req.Channel, req.SubjectTemplate, req.BodyTemplate)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, service.ErrInvalidRequest) {
			status = http.StatusBadRequest
		}
		httputil.WriteError(w, httputil.NewAppError(status, "CREATE_FAILED", err.Error(), err))
		return
	}
	httputil.WriteJSON(w, http.StatusCreated, t)
}

func (h *Handler) ListTemplates(w http.ResponseWriter, r *http.Request) {
	list, err := h.svc.ListTemplates(r.Context())
	if err != nil {
		httputil.WriteError(w, httputil.NewAppError(http.StatusInternalServerError, "INTERNAL", "list failed", err))
		return
	}
	httputil.WriteJSON(w, http.StatusOK, list)
}

func (h *Handler) Send(w http.ResponseWriter, r *http.Request) {
	var req model.SendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteError(w, httputil.NewAppError(http.StatusBadRequest, "BAD_REQUEST", "invalid json", err))
		return
	}
	if key := r.Header.Get("Idempotency-Key"); key != "" && req.IdempotencyKey == "" {
		req.IdempotencyKey = key
	}
	n, err := h.svc.Send(r.Context(), req)
	if err != nil {
		status := http.StatusInternalServerError
		switch {
		case errors.Is(err, service.ErrInvalidRequest):
			status = http.StatusBadRequest
		case errors.Is(err, service.ErrTemplateNotFound):
			status = http.StatusNotFound
		}
		httputil.WriteError(w, httputil.NewAppError(status, "SEND_FAILED", err.Error(), err))
		return
	}
	httputil.WriteJSON(w, http.StatusAccepted, n)
}

func (h *Handler) GetNotification(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		httputil.WriteError(w, httputil.NewAppError(http.StatusBadRequest, "BAD_REQUEST", "invalid id", err))
		return
	}
	n, err := h.svc.GetNotification(r.Context(), id)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, service.ErrNotFound) {
			status = http.StatusNotFound
		}
		httputil.WriteError(w, httputil.NewAppError(status, "GET_FAILED", err.Error(), err))
		return
	}
	httputil.WriteJSON(w, http.StatusOK, n)
}

func (h *Handler) ListAttempts(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		httputil.WriteError(w, httputil.NewAppError(http.StatusBadRequest, "BAD_REQUEST", "invalid id", err))
		return
	}
	list, err := h.svc.ListAttempts(r.Context(), id)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, service.ErrNotFound) {
			status = http.StatusNotFound
		}
		httputil.WriteError(w, httputil.NewAppError(status, "LIST_FAILED", err.Error(), err))
		return
	}
	httputil.WriteJSON(w, http.StatusOK, list)
}
