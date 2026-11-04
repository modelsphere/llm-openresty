//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	namespace    = "openresty"
	routerDeploy = "openresty-router"
	mockDeploy   = "mock-upstream"
	routerSvc    = "openresty-router"

	defaultReadySeconds = 300
	defaultLocalPort    = 18080 // -> svc 8080 (path dispatch)
	defaultAdminPort    = 18090 // -> svc 8090 (admin/health)
)

type suiteConfig struct {
	kindBin      string
	kubectlBin   string
	dockerBin    string
	cluster      string
	context      string
	image        string
	skipBuild    bool
	readyTimeout time.Duration
	localPort    int
	adminPort    int
}

var (
	cfg             suiteConfig
	root            string
	portForward     *exec.Cmd
	portForwardDone chan struct{}
	beforeSuiteOK   bool
)

func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "llm-openresty e2e")
}

var _ = BeforeSuite(func() {
	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	cfg = loadConfig()

	for _, bin := range []string{cfg.kindBin, cfg.kubectlBin, cfg.dockerBin} {
		_, err := exec.LookPath(bin)
		Expect(err).NotTo(HaveOccurred(), "%s is not installed", bin)
	}

	var err error
	root, err = repoRoot()
	Expect(err).NotTo(HaveOccurred())

	if cfg.skipBuild {
		_, _ = fmt.Fprintf(GinkgoWriter, "SKIP_BUILD set: not building %s\n", cfg.image)
	} else {
		By("building the openresty image")
		cmd := exec.Command("make", "docker-build", "IMG="+cfg.image)
		out, err := run(cmd)
		Expect(err).NotTo(HaveOccurred(), out)
	}
	By("loading the openresty image into kind")
	Expect(loadImageToKind(cfg.image)).To(Succeed())

	installMockAndRouter()
	startPortForwards()
	beforeSuiteOK = true
})

var _ = AfterSuite(func() {
	stopPortForward()
	if !beforeSuiteOK {
		dumpClusterState("BeforeSuite")
	}
})

func loadConfig() suiteConfig {
	c := suiteConfig{
		kindBin:      envOr("KIND", "kind"),
		kubectlBin:   envOr("KUBECTL", "kubectl"),
		dockerBin:    envOr("DOCKER", "docker"),
		cluster:      envOr("KIND_CLUSTER", "openresty-test-e2e"),
		image:        envOr("IMG", "llm-openresty:e2e"),
		readyTimeout: time.Duration(envInt("READY_TIMEOUT", defaultReadySeconds)) * time.Second,
		localPort:    envInt("LOCAL_PORT", defaultLocalPort),
		adminPort:    envInt("ADMIN_PORT", defaultAdminPort),
	}
	c.context = "kind-" + c.cluster
	c.skipBuild = os.Getenv("SKIP_BUILD") == "true" || os.Getenv("SKIP_BUILD") == "1"
	return c
}

func envOr(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	Expect(err).NotTo(HaveOccurred(), "%s=%q is not an integer", key, v)
	return n
}

// repoRoot walks up from the test working directory to the checkout root,
// marked by the image Dockerfile and this suite's go.mod.
func repoRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	dir := wd
	for {
		if fileExists(filepath.Join(dir, "Dockerfile")) && fileExists(filepath.Join(dir, "test", "e2e", "go.mod")) {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("repo root not found above %s", wd)
		}
		dir = parent
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func installMockAndRouter() {
	By("creating namespace " + namespace)
	genNs := exec.Command(cfg.kubectlBin, "--context", cfg.context,
		"create", "namespace", namespace, "--dry-run=client", "-o", "yaml")
	genNs.Dir = root
	nsManifest, err := genNs.Output()
	Expect(err).NotTo(HaveOccurred())
	applyNs := exec.Command(cfg.kubectlBin, "--context", cfg.context, "apply", "-f", "-")
	applyNs.Stdin = bytes.NewReader(nsManifest)
	out, err := run(applyNs)
	Expect(err).NotTo(HaveOccurred(), out)

	By("applying testdata/k8s.yaml")
	mustKubectl("apply", "-f", filepath.Join(root, "test", "e2e", "testdata", "k8s.yaml"))

	By("pointing the router deployment at " + cfg.image)
	mustKubectl("-n", namespace, "set", "image", "deploy/"+routerDeploy, "openresty="+cfg.image)

	By("waiting for the mock upstream")
	mustKubectl("-n", namespace, "rollout", "status", "deploy/"+mockDeploy,
		"--timeout="+cfg.readyTimeout.String())
	By("waiting for the router")
	mustKubectl("-n", namespace, "rollout", "status", "deploy/"+routerDeploy,
		"--timeout="+cfg.readyTimeout.String())
}

func startPortForwards() {
	By(fmt.Sprintf("port-forwarding svc/%s %d:8080 %d:8090", routerSvc, cfg.localPort, cfg.adminPort))
	cmd := exec.Command(cfg.kubectlBin, "--context", cfg.context,
		"-n", namespace, "port-forward", "svc/"+routerSvc,
		fmt.Sprintf("%d:8080", cfg.localPort), fmt.Sprintf("%d:8090", cfg.adminPort))
	setPortForwardAttr(cmd)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	Expect(cmd.Start()).To(Succeed())
	portForward = cmd
	portForwardDone = make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(portForwardDone)
	}()

	Eventually(func(g Gomega) {
		select {
		case <-portForwardDone:
			g.Expect(true).To(BeFalse(), "port-forward exited; are ports %d/%d free? set LOCAL_PORT/ADMIN_PORT", cfg.localPort, cfg.adminPort)
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet,
			fmt.Sprintf("http://127.0.0.1:%d/healthz", cfg.adminPort), nil)
		g.Expect(err).NotTo(HaveOccurred())
		resp, err := http.DefaultClient.Do(req)
		g.Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		g.Expect(resp.StatusCode).To(Equal(http.StatusOK))
	}).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())
}

func stopPortForward() {
	if portForward == nil || portForward.Process == nil {
		return
	}
	_ = killPortForward(portForward)
	if portForwardDone != nil {
		select {
		case <-portForwardDone:
		case <-time.After(5 * time.Second):
		}
	}
	portForward = nil
}

func run(cmd *exec.Cmd) (string, error) {
	if cmd.Dir == "" {
		cmd.Dir = root
	}
	command := strings.Join(cmd.Args, " ")
	_, _ = fmt.Fprintf(GinkgoWriter, "running: %s\n", command)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return string(output), fmt.Errorf("%s failed: %w\n%s", command, err, output)
	}
	return string(output), nil
}

func loadImageToKind(image string) error {
	cmd := exec.Command(cfg.kindBin, "load", "docker-image", image, "--name", cfg.cluster)
	_, err := run(cmd)
	return err
}

func kubectl(args ...string) (string, error) {
	cmd := exec.Command(cfg.kubectlBin, append([]string{"--context", cfg.context}, args...)...)
	return run(cmd)
}

func mustKubectl(args ...string) {
	out, err := kubectl(args...)
	Expect(err).NotTo(HaveOccurred(), out)
}

func dumpClusterState(spec string) {
	_, _ = fmt.Fprintf(GinkgoWriter, "\n==> dumping cluster state (%s)\n", spec)
	dumpCmd := func(title string, args ...string) {
		out, err := kubectl(args...)
		_, _ = fmt.Fprintf(GinkgoWriter, "\n--- %s\n", title)
		if out != "" {
			_, _ = fmt.Fprintf(GinkgoWriter, "%s\n", out)
		}
		if err != nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "error: %v\n", err)
		}
	}
	dumpCmd("pods", "get", "pods", "-A", "-o", "wide")
	out, err := kubectl("get", "events", "-n", namespace, "--sort-by=.lastTimestamp")
	_, _ = fmt.Fprintf(GinkgoWriter, "\n--- events\n%s\n", tail(out, 60))
	if err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "events error: %v\n", err)
	}
	dumpCmd("router logs", "-n", namespace, "logs", "deploy/"+routerDeploy, "--tail=200")
	dumpCmd("mock logs", "-n", namespace, "logs", "deploy/"+mockDeploy, "--tail=100")
}

func tail(s string, n int) string {
	lines := strings.Split(s, "\n")
	if len(lines) <= n {
		return s
	}
	return strings.Join(lines[len(lines)-n:], "\n")
}
