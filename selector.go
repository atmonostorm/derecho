package derecho

type Selector struct {
	cases  []*selectorCase
	winner *selectorCase
}

type selectorCase struct {
	run    func(ctx Context, sel *Selector)
	invoke func()
	fired  bool
}

func NewSelector() *Selector {
	return &Selector{}
}

func (s *Selector) HasPending() bool {
	for _, c := range s.cases {
		if !c.fired {
			return true
		}
	}
	return false
}

func (s *Selector) Select(ctx Context) {
	pending := 0
	for _, c := range s.cases {
		if !c.fired {
			pending++
		}
	}
	if pending == 0 {
		return
	}

	s.winner = nil

	for _, c := range s.cases {
		if c.fired {
			continue
		}
		cas := c
		Go(ctx, func(ctx Context) {
			cas.run(ctx, s)
		})
	}

	Await(ctx, func() bool {
		return s.winner != nil
	})

	s.winner.fired = true
	s.winner.invoke()
}

func AddFuture[T any](s *Selector, f Future[T], callback func(T, error)) *Selector {
	var result T
	var err error

	cas := &selectorCase{}
	cas.run = func(ctx Context, sel *Selector) {
		result, err = f.Get(ctx)
		if sel.winner == nil {
			sel.winner = cas
			if ps, ok := ctx.(progressSignaler); ok {
				ps.signalProgress()
			}
		}
	}
	cas.invoke = func() {
		callback(result, err)
	}

	s.cases = append(s.cases, cas)
	return s
}
