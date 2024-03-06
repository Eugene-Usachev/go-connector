package pipe

type Slot struct {
	ch chan *Res
}

func NewSlot() *Slot {
	return &Slot{
		ch: make(chan *Res, 1),
	}
}

func (s *Slot) Wait() Res {
	res := <-s.ch
	return *res
}

func (s *Slot) Set(res *Res) {
	s.ch <- res
}
