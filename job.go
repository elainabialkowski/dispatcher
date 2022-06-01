package daemon

type Job interface {
	Validate() error
	Run() error
}
