package journal

import (
	"testing"
	"time"
)

func TestMarshalUnmarshalEventsJSON(t *testing.T) {
	now := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	original := []Event{
		WorkflowStarted{BaseEvent: BaseEvent{ID: 1}, WorkflowType: "order", Args: []byte(`{"id":123}`), StartedAt: now},
		WorkflowTaskScheduled{BaseEvent: BaseEvent{ID: 2, ScheduledByID: 1}},
		WorkflowTaskStarted{BaseEvent: BaseEvent{ID: 3, ScheduledByID: 2}, WorkerID: "w1", StartedAt: now},
		ActivityScheduled{BaseEvent: BaseEvent{ID: 4, ScheduledByID: 3}, Name: "fetchOrder", Input: []byte(`{"id":123}`)},
		ActivityStarted{BaseEvent: BaseEvent{ID: 5, ScheduledByID: 4}, WorkerID: "w1", StartedAt: now},
		ActivityCompleted{BaseEvent: BaseEvent{ID: 6, ScheduledByID: 4}, Result: []byte(`{"status":"ok"}`)},
		WorkflowTaskCompleted{BaseEvent: BaseEvent{ID: 7, ScheduledByID: 2}},
		TimerScheduled{BaseEvent: BaseEvent{ID: 8, ScheduledByID: 3}, Duration: 5 * time.Second, FireAt: now.Add(5 * time.Second)},
		TimerFired{BaseEvent: BaseEvent{ID: 9, ScheduledByID: 8}, FiredAt: now.Add(5 * time.Second)},
		SideEffectRecorded{BaseEvent: BaseEvent{ID: 10, ScheduledByID: 3}, Result: []byte(`"uuid-456"`)},
		ActivityFailed{BaseEvent: BaseEvent{ID: 11, ScheduledByID: 4}, Error: NewError(ErrorKindTimeout, "timed out")},
		WorkflowFailed{BaseEvent: BaseEvent{ID: 12, ScheduledByID: 1}, Error: NewError(ErrorKindApplication, "failed")},
		WorkflowCompleted{BaseEvent: BaseEvent{ID: 13, ScheduledByID: 1}, Result: []byte(`"done"`)},
		ChildWorkflowScheduled{BaseEvent: BaseEvent{ID: 14, ScheduledByID: 3}, WorkflowType: "child", WorkflowID: "c1", Input: []byte(`{}`), ParentWorkflowID: "p1", ParentRunID: "r1"},
		ChildWorkflowStarted{BaseEvent: BaseEvent{ID: 15, ScheduledByID: 14}, ChildRunID: "cr1", StartedAt: now},
		ChildWorkflowCompleted{BaseEvent: BaseEvent{ID: 16, ScheduledByID: 14}, ChildWorkflowID: "c1", ChildRunID: "cr1", Result: []byte(`"child done"`)},
		ChildWorkflowFailed{BaseEvent: BaseEvent{ID: 17, ScheduledByID: 14}, ChildWorkflowID: "c1", ChildRunID: "cr1", Error: NewError(ErrorKindApplication, "child err")},
		TimerCancelled{BaseEvent: BaseEvent{ID: 18, ScheduledByID: 8}},
	}

	data, err := MarshalEventsJSON(original)
	if err != nil {
		t.Fatalf("MarshalEventsJSON: %v", err)
	}

	restored, err := UnmarshalEventsJSON(data)
	if err != nil {
		t.Fatalf("UnmarshalEventsJSON: %v", err)
	}

	if len(restored) != len(original) {
		t.Fatalf("len(restored) = %d, want %d", len(restored), len(original))
	}

	for i, ev := range restored {
		orig := original[i]
		base := ev.Base()
		origBase := orig.Base()

		if base.ID != origBase.ID {
			t.Errorf("event %d: ID = %d, want %d", i, base.ID, origBase.ID)
		}
		if base.ScheduledByID != origBase.ScheduledByID {
			t.Errorf("event %d: ScheduledByID = %d, want %d", i, base.ScheduledByID, origBase.ScheduledByID)
		}
		if ev.EventType() != orig.EventType() {
			t.Errorf("event %d: EventType = %q, want %q", i, ev.EventType(), orig.EventType())
		}
	}

	// Verify concrete types round-trip correctly
	if ws, ok := restored[0].(WorkflowStarted); !ok {
		t.Errorf("event 0: type = %T, want WorkflowStarted", restored[0])
	} else if ws.WorkflowType != "order" {
		t.Errorf("WorkflowStarted.WorkflowType = %q, want %q", ws.WorkflowType, "order")
	}

	if as, ok := restored[3].(ActivityScheduled); !ok {
		t.Errorf("event 3: type = %T, want ActivityScheduled", restored[3])
	} else if as.Name != "fetchOrder" {
		t.Errorf("ActivityScheduled.Name = %q, want %q", as.Name, "fetchOrder")
	}

	if af, ok := restored[10].(ActivityFailed); !ok {
		t.Errorf("event 10: type = %T, want ActivityFailed", restored[10])
	} else if af.Error.Kind != ErrorKindTimeout {
		t.Errorf("ActivityFailed.Error.Kind = %q, want %q", af.Error.Kind, ErrorKindTimeout)
	}

	if se, ok := restored[9].(SideEffectRecorded); !ok {
		t.Errorf("event 9: type = %T, want SideEffectRecorded", restored[9])
	} else if string(se.Result) != `"uuid-456"` {
		t.Errorf("SideEffectRecorded.Result = %s, want %q", se.Result, `"uuid-456"`)
	}

	if cws, ok := restored[13].(ChildWorkflowScheduled); !ok {
		t.Errorf("event 13: type = %T, want ChildWorkflowScheduled", restored[13])
	} else if cws.WorkflowType != "child" {
		t.Errorf("ChildWorkflowScheduled.WorkflowType = %q, want %q", cws.WorkflowType, "child")
	}

	if _, ok := restored[17].(TimerCancelled); !ok {
		t.Errorf("event 17: type = %T, want TimerCancelled", restored[17])
	}
}

func TestUnmarshalEventsJSON_UnknownType(t *testing.T) {
	data := []byte(`[{"type":"unknown_type","id":1}]`)
	_, err := UnmarshalEventsJSON(data)
	if err == nil {
		t.Fatal("expected error for unknown type")
	}
}

func TestUnmarshalEventsJSON_InvalidJSON(t *testing.T) {
	_, err := UnmarshalEventsJSON([]byte(`not json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}
