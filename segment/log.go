package segment

type Log struct {
	logFile *BlockFile
	seq    uint64
	offset uint64
}

func (ex Extent) Replay() {

}
