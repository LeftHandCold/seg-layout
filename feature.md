# Segmnet Layout Feature

## 概述

本文描述Segment Layout模块需要对外提供所以的需求。

## Feature

### Append

提供Append(fd *block, []byte)接口，向block文件追加写入数据，需4k对齐。

### Update

提供Update(fd *block, Batch)接口, 更新block文件中page数据，batch包含new page，更新page的offset lenth，
还有update操作的描述数据，如时间戳等等。这些所有数据最好写入连续的page中。

### VersionSet

Layout层需要生成一个版本链，来记录每次update的数据，直到通知版本链丢弃指定版本，Layout才会释放被丢弃数据占用的空间。

### Sync

提供Sync接口，确保数据和原数据落盘。

### NewFile

提供NewFile(fname string)接口，来创建需要用到的block file，和一些记录特殊信息的文件（比如记录segment少量更新的row，而不是指定page的数据）。
可以根据文件名来区分写入数据的type。
