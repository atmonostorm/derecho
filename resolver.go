package derecho

// WorkflowResolver looks up workflow functions by name. Returned fn must have
// signature func(Context, I) (O, error) - use RegisterWorkflow to ensure this.
type WorkflowResolver interface {
	ResolveWorkflow(name string) (fn any, ok bool)
}

// ActivityResolver looks up activity functions by name. Returned fn must have
// signature func(context.Context, I) (O, error) - use RegisterActivity to ensure this.
type ActivityResolver interface {
	ResolveActivity(name string) (fn any, ok bool)
}
