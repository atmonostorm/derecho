package journal

import (
	"encoding/json"
	"fmt"
)

// rawEvent is used for two-phase JSON unmarshaling.
type rawEvent struct {
	Type string `json:"type"`
}

// MarshalEventsJSON serializes a slice of events to JSON.
func MarshalEventsJSON(events []Event) ([]byte, error) {
	// Wrap each event with its type for polymorphic deserialization
	wrapped := make([]json.RawMessage, len(events))
	for i, ev := range events {
		data, err := json.Marshal(ev)
		if err != nil {
			return nil, fmt.Errorf("marshal event %d: %w", i, err)
		}

		// Inject the type field
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, fmt.Errorf("unmarshal event %d for type injection: %w", i, err)
		}
		m["type"] = ev.EventType()

		data, err = json.Marshal(m)
		if err != nil {
			return nil, fmt.Errorf("remarshal event %d: %w", i, err)
		}
		wrapped[i] = data
	}
	return json.Marshal(wrapped)
}

// UnmarshalEventsJSON deserializes a JSON array into typed events.
func UnmarshalEventsJSON(data []byte) ([]Event, error) {
	var raws []json.RawMessage
	if err := json.Unmarshal(data, &raws); err != nil {
		return nil, fmt.Errorf("unmarshal events array: %w", err)
	}

	events := make([]Event, len(raws))
	for i, raw := range raws {
		ev, err := unmarshalEvent(raw)
		if err != nil {
			return nil, fmt.Errorf("unmarshal event %d: %w", i, err)
		}
		events[i] = ev
	}
	return events, nil
}

func unmarshalEvent(data []byte) (Event, error) {
	var r rawEvent
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("unmarshal type: %w", err)
	}

	var ev Event
	switch r.Type {
	case TypeWorkflowStarted:
		ev = &WorkflowStarted{}
	case TypeWorkflowCompleted:
		ev = &WorkflowCompleted{}
	case TypeWorkflowFailed:
		ev = &WorkflowFailed{}
	case TypeWorkflowCancelRequested:
		ev = &WorkflowCancelRequested{}
	case TypeWorkflowCancelled:
		ev = &WorkflowCancelled{}
	case TypeWorkflowTaskScheduled:
		ev = &WorkflowTaskScheduled{}
	case TypeWorkflowTaskStarted:
		ev = &WorkflowTaskStarted{}
	case TypeWorkflowTaskCompleted:
		ev = &WorkflowTaskCompleted{}
	case TypeActivityScheduled:
		ev = &ActivityScheduled{}
	case TypeActivityStarted:
		ev = &ActivityStarted{}
	case TypeActivityCompleted:
		ev = &ActivityCompleted{}
	case TypeActivityFailed:
		ev = &ActivityFailed{}
	case TypeTimerScheduled:
		ev = &TimerScheduled{}
	case TypeTimerFired:
		ev = &TimerFired{}
	case TypeTimerCancelled:
		ev = &TimerCancelled{}
	case TypeSideEffectRecorded:
		ev = &SideEffectRecorded{}
	case TypeChildWorkflowScheduled:
		ev = &ChildWorkflowScheduled{}
	case TypeChildWorkflowStarted:
		ev = &ChildWorkflowStarted{}
	case TypeChildWorkflowCompleted:
		ev = &ChildWorkflowCompleted{}
	case TypeChildWorkflowFailed:
		ev = &ChildWorkflowFailed{}
	case TypeSignalReceived:
		ev = &SignalReceived{}
	case TypeSignalExternalScheduled:
		ev = &SignalExternalScheduled{}
	case TypeWorkflowContinuedAsNew:
		ev = &WorkflowContinuedAsNew{}
	default:
		return nil, fmt.Errorf("unknown event type %q", r.Type)
	}

	if err := json.Unmarshal(data, ev); err != nil {
		return nil, fmt.Errorf("unmarshal %s: %w", r.Type, err)
	}

	// Dereference pointer to return value type
	switch e := ev.(type) {
	case *WorkflowStarted:
		return *e, nil
	case *WorkflowCompleted:
		return *e, nil
	case *WorkflowFailed:
		return *e, nil
	case *WorkflowCancelRequested:
		return *e, nil
	case *WorkflowCancelled:
		return *e, nil
	case *WorkflowTaskScheduled:
		return *e, nil
	case *WorkflowTaskStarted:
		return *e, nil
	case *WorkflowTaskCompleted:
		return *e, nil
	case *ActivityScheduled:
		return *e, nil
	case *ActivityStarted:
		return *e, nil
	case *ActivityCompleted:
		return *e, nil
	case *ActivityFailed:
		return *e, nil
	case *TimerScheduled:
		return *e, nil
	case *TimerFired:
		return *e, nil
	case *TimerCancelled:
		return *e, nil
	case *SideEffectRecorded:
		return *e, nil
	case *ChildWorkflowScheduled:
		return *e, nil
	case *ChildWorkflowStarted:
		return *e, nil
	case *ChildWorkflowCompleted:
		return *e, nil
	case *ChildWorkflowFailed:
		return *e, nil
	case *SignalReceived:
		return *e, nil
	case *SignalExternalScheduled:
		return *e, nil
	case *WorkflowContinuedAsNew:
		return *e, nil
	default:
		return ev, nil
	}
}
