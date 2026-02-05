package openai

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/ai"
	"github.com/nyaruka/mailroom/core/models"
	oai "github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
	"github.com/openai/openai-go/shared"
)

const (
	TypeOpenAI   = "openai"
	TypeOpenClaw = "openclaw"

	configAPIKey   = "api_key"
	configEndpoint = "endpoint"
)

func init() {
	models.RegisterLLMService(TypeOpenAI, New)
	models.RegisterLLMService(TypeOpenClaw, New)
}

// an LLM service implementation for OpenAI
type service struct {
	client oai.Client
	model  string
}

func New(m *models.LLM, c *http.Client) (flows.LLMService, error) {
	apiKey := m.Config().GetString(configAPIKey, "")
	if apiKey == "" {
		return nil, fmt.Errorf("config incomplete for LLM: %s", m.UUID())
	}

	opts := []option.RequestOption{
		option.WithAPIKey(apiKey),
		option.WithHTTPClient(c),
	}

	if endpoint := m.Config().GetString(configEndpoint, ""); endpoint != "" {
		opts = append(opts, option.WithBaseURL(endpoint))
	}

	return &service{
		client: oai.NewClient(opts...),
		model:  m.Model(),
	}, nil
}

func (s *service) Response(ctx context.Context, instructions, input string, maxTokens int) (*flows.LLMResponse, error) {
	var messages []oai.ChatCompletionMessageParamUnion

	// Add system prompt if present
	if instructions != "" {
		messages = append(messages, oai.SystemMessage(instructions))
	}

	// Try to parse input as JSON to see if it contains structured messages
	// This enables "Experimental Multi-Turn" support as per OpenClaw docs
	var inputPayload struct {
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}

	isJSON := false
	if strings.TrimSpace(input) != "" && strings.HasPrefix(strings.TrimSpace(input), "{") {
		if err := json.Unmarshal([]byte(input), &inputPayload); err == nil && len(inputPayload.Messages) > 0 {
			isJSON = true
			for _, msg := range inputPayload.Messages {
				switch msg.Role {
				case "system":
					messages = append(messages, oai.SystemMessage(msg.Content))
				case "assistant":
					messages = append(messages, oai.AssistantMessage(msg.Content))
				case "user":
					fallthrough
				default:
					messages = append(messages, oai.UserMessage(msg.Content))
				}
			}
		}
	}

	// Fallback: simple text input -> User Message
	if !isJSON && strings.TrimSpace(input) != "" {
		messages = append(messages, oai.UserMessage(input))
	}

	resp, err := s.client.Chat.Completions.New(ctx, oai.ChatCompletionNewParams{
		Model:       shared.ChatModel(s.model),
		Messages:    messages,
		Temperature: oai.Float(0.000001),
		MaxTokens:   oai.Int(int64(maxTokens)),
	})
	if err != nil {
		return nil, s.error(err, instructions, input)
	}

	if len(resp.Choices) == 0 {
		return &flows.LLMResponse{Output: "", TokensUsed: 0}, nil
	}

	return &flows.LLMResponse{
		Output:     resp.Choices[0].Message.Content,
		TokensUsed: resp.Usage.TotalTokens,
	}, nil
}

func (s *service) error(err error, instructions, input string) error {
	code := ai.ErrorUnknown
	var aerr *oai.Error
	if errors.As(err, &aerr) {
		if aerr.StatusCode == http.StatusUnauthorized {
			code = ai.ErrorCredentials
		} else if aerr.StatusCode == http.StatusTooManyRequests {
			code = ai.ErrorRateLimit
		}
	}
	return &ai.ServiceError{Message: err.Error(), Code: code, Instructions: instructions, Input: input}
}
