package main

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

var rnd = rand.New(rand.NewSource(42))

// 2026-09-15 事故的回归测试。当时:
//   · housekeep 的守卫是 os.Stat(dst),而 dst 要等打包完成 rename 才出现 → 挡不住并发;
//   · tarGzDir 的临时文件是固定名 dst+".tmp" → 两个并发调用写同一个 inode,
//     先完成的 rename 成正式归档后,另一个继续把**正式归档**写坏;
//   · 调用方随后 RemoveAll 源目录 → 归档半截 + 源文件已删 = 永久丢数据。
// 生产上连续七天、每天丢 8~14 小时的 bodylog。

func mkDay(t *testing.T, root, day string, hours int) string {
	t.Helper()
	dir := filepath.Join(root, day)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for h := 0; h < hours; h++ {
		p := filepath.Join(dir, fmt.Sprintf("%02d.jsonl", h))
		// ⚠️ 必须足够大且**不可压缩**(随机数据),否则两个 goroutine 瞬间跑完、
		// 竞态窗口根本不打开,测试就成了摆设 —— 第一版用 24×256KB 的重复字节,
		// 拿去打事故版代码照样 PASS(2026-09-15 踩)。
		buf := make([]byte, 12*1024*1024)
		rnd.Read(buf)
		if err := os.WriteFile(p, buf, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func countMembers(t *testing.T, path string) int {
	t.Helper()
	n, err := readMembers(path)
	if err != nil {
		t.Fatalf("归档读坏了(只读出前 %d 个成员): %v", n, err)
	}
	return n
}

// readMembers 完整读一遍归档,返回成功读出的成员数和错误(坏档时能看出读到哪儿崩的)。
func readMembers(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	gr, err := gzip.NewReader(f)
	if err != nil {
		return 0, err
	}
	defer gr.Close()
	tr := tar.NewReader(gr)
	n := 0
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return n, nil
		}
		if err != nil {
			return n, err
		}
		if _, err := io.Copy(io.Discard, tr); err != nil {
			return n, err
		}
		if !hdr.FileInfo().IsDir() {
			n++
		}
	}
}

func TestTarGzDirConcurrentSameDst(t *testing.T) {
	root := t.TempDir()
	src := mkDay(t, root, "2026-09-13", 24)
	dst := filepath.Join(root, "2026-09-13.tar.gz")

	// 两个并发调用打同一个目标 —— 事故现场的形状
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = tarGzDir(src, dst)
		}(i)
	}
	wg.Wait()

	if errs[0] != nil && errs[1] != nil {
		t.Fatalf("两个都失败了(至少该有一个成功): %v / %v", errs[0], errs[1])
	}
	if n := countMembers(t, dst); n != 24 {
		t.Fatalf("归档成员数 %d,应为 24 —— 并发把归档写坏了", n)
	}
}

func TestTarGzDirVerifyCatchesMissing(t *testing.T) {
	// 自检必须能发现"源目录里有、归档里没有"。模拟打包过程中文件被删:
	// 先正常打一个 23 小时的包,再把源目录补到 24 小时,自检应当报少成员。
	root := t.TempDir()
	src := mkDay(t, root, "2026-09-14", 23)
	dst := filepath.Join(root, "2026-09-14.tar.gz")
	if err := tarGzDir(src, dst); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "23.jsonl"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := verifyTarGz(dst, src); err == nil {
		t.Fatal("自检没发现归档少了 23.jsonl —— 这道闸就白加了")
	}
}

func TestHousekeepStaggeredConcurrent(t *testing.T) {
	// ⚠️ 复现的关键是**第二个 housekeep 起得够晚**(实测:早了反而复现不出来)。
	// 机制:B 的 os.Create(同名 tmp) 是 O_TRUNC,把 A 写了一大半的文件截成 0;B 只跑到
	// 更靠前的位置就被 A 的 RemoveAll 打断 —— 中间那段**从没被写过**,是空洞。
	// A 的 fd 偏移仍在高位,尾巴落在空洞之后。从头读到 B 停下的地方就撞零字节 →
	// gzip "invalid stored block lengths" / "format violated"。
	// B 若起得早、能把空洞填满,两条流字节完全相同,覆盖看不出问题,归档反而是好的。
	// 生产:A 在 76%(19/25 分钟)时被截断,B 只到 42%(10/24 小时)就死 → 空洞 42%~76%。
	//
	// 所以这里**先量一次单独打包的耗时,再按 75% 错开**,而不是拍一个固定的 sleep。
	root := t.TempDir()
	mkDay(t, root, "2026-09-13", 24)
	src := filepath.Join(root, "2026-09-13")

	probe := filepath.Join(root, "probe.tar.gz")
	t1 := time.Now()
	if err := tarGzDir(src, probe); err != nil {
		t.Fatal(err)
	}
	solo := time.Since(t1)
	_ = os.Remove(probe)
	t.Logf("单独打包耗时 %v,B 将在 %v 后启动(75%%)", solo, solo*3/4)

	var wg sync.WaitGroup
	t0 := time.Now()
	wg.Add(1)
	go func() { defer wg.Done(); housekeep(root); t.Logf("A 结束 @%v", time.Since(t0)) }()
	time.Sleep(solo * 3 / 4)
	wg.Add(1)
	go func() { defer wg.Done(); housekeep(root); t.Logf("B 结束 @%v", time.Since(t0)) }()
	wg.Wait()

	dst := filepath.Join(root, "2026-09-13.tar.gz")
	srcGone := false
	if _, err := os.Stat(src); os.IsNotExist(err) {
		srcGone = true
	}
	if _, err := os.Stat(dst); err != nil {
		if !srcGone {
			return // 自检拦下了,源目录保住 —— 可接受
		}
		t.Fatal("归档没生成、源目录也没了 = 数据丢失")
	}
	n, err := readMembers(dst)
	if err != nil {
		t.Fatalf("归档读坏了(只读出前 %d 个成员)= 事故复现: %v;源目录是否已删=%v", n, err, srcGone)
	}
	if n != 24 {
		t.Fatalf("归档成员数 %d,应为 24;源目录是否已删=%v", n, srcGone)
	}
}
