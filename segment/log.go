package segment

type Log struct {
	writer *FileWriter
	seq    uint64
}

func (ex Extent) Replay() {

}
