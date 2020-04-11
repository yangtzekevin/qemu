# High-level test for qemu COLO testing all failover cases while checking
# guest network connectivity
#
# Copyright (c) Lukas Straub <lukasstraub2@web.de>
#
# This work is licensed under the terms of the GNU GPL, version 2 or
# later.  See the COPYING file in the top-level directory.

import select
import sys
import subprocess
import shutil
import os
import signal
import os.path
import time
import tempfile

from avocado import Test
from avocado import skipUnless
from avocado.utils import network
from avocado.utils import process
from avocado.utils import vmimage
from avocado.utils import cloudinit
from avocado.utils import ssh
from avocado.utils.path import find_command

from avocado_qemu import pick_default_qemu_bin, SRC_ROOT_DIR

BUILD_DIR = SRC_ROOT_DIR
SOURCE_DIR = SRC_ROOT_DIR

def iperf3_available():
    try:
        find_command("iperf3")
    except CmdNotFoundError:
        return False
    return True

class ColoTest(Test):

    # Constants
    OCF_SUCCESS = 0
    OCF_ERR_GENERIC = 1
    OCF_ERR_ARGS = 2
    OCF_ERR_UNIMPLEMENTED = 3
    OCF_ERR_PERM = 4
    OCF_ERR_INSTALLED = 5
    OCF_ERR_CONFIGURED = 6
    OCF_NOT_RUNNING = 7
    OCF_RUNNING_MASTER = 8
    OCF_FAILED_MASTER = 9

    HOSTA = 10
    HOSTB = 11

    # TODO: add rng for hostst without rng instructions
    QEMU_OPTIONS = (" -display none -vga none -enable-kvm"
                    " -smp 2 -cpu host -m 768"
                    " -device e1000,mac=52:54:00:12:34:56,netdev=hn0"
                    " -device virtio-blk,drive=colo-disk0")

    FEDORA_VERSION = "31"
    RPMFUSION_URL = "https://download1.rpmfusion.org/free/fedora/rpmfusion-free-release-31.noarch.rpm"
    IMAGE_CHECKSUM = "e3c1b309d9203604922d6e255c2c5d098a309c2d46215d8fc026954f3c5c27a0"
    IMAGE_SIZE = "4294967296b"

    hang_qemu = False
    traffic_procs = []

    def get_image(self, temp_dir):
        try:
            return vmimage.get(
                "fedora", arch="x86_64", version=self.FEDORA_VERSION,
                checksum=self.IMAGE_CHECKSUM, algorithm="sha256",
                cache_dir=self.cache_dirs[0],
                snapshot_dir=temp_dir)
        except:
            self.cancel("Failed to download/prepare image")

    @skipUnless(ssh.SSH_CLIENT_BINARY, "No SSH client available")
    def setUp(self):
        # Qemu and qemu-img binary
        default_qemu_bin = pick_default_qemu_bin()
        self.QEMU_BINARY = self.params.get("qemu_bin", default=default_qemu_bin)

        # If qemu-img has been built, use it, otherwise the system wide one
        # will be used.  If none is available, the test will cancel.
        qemu_img = os.path.join(BUILD_DIR, "qemu-img")
        if not os.path.exists(qemu_img):
            qemu_img = find_command("qemu-img", False)
        if qemu_img is False:
            self.cancel("Could not find \"qemu-img\", which is required to "
                        "create the bootable image")
        self.QEMU_IMG_BINARY = qemu_img
        vmimage.QEMU_IMG = qemu_img

        self.RESOURCE_AGENT = os.path.join(SOURCE_DIR,
                                           "scripts/colo-resource-agent/colo")
        self.ADD_PATH = os.path.join(SOURCE_DIR, "scripts/colo-resource-agent")

        # Logs
        self.RA_LOG = os.path.join(self.outputdir, "resource-agent.log")
        self.HOSTA_LOGDIR = os.path.join(self.outputdir, "hosta")
        self.HOSTB_LOGDIR = os.path.join(self.outputdir, "hostb")
        os.makedirs(self.HOSTA_LOGDIR)
        os.makedirs(self.HOSTB_LOGDIR)

        # Temporary directories
        # We don't use self.workdir because of unix socket path length
        # limitations
        self.TMPDIR = tempfile.mkdtemp()
        self.TMPA = os.path.join(self.TMPDIR, "hosta")
        self.TMPB = os.path.join(self.TMPDIR, "hostb")
        os.makedirs(self.TMPA)
        os.makedirs(self.TMPB)

        # Network
        self.BRIDGE_NAME = self.params.get("bridge_name")
        if self.BRIDGE_NAME:
            self.HOST_ADDRESS = self.params.get("host_address")
            self.GUEST_ADDRESS = self.params.get("guest_address")
            self.BRIDGE_HELPER = self.params.get("bridge_helper",
                                    default="/usr/lib/qemu/qemu-bridge-helper")
            self.SSH_PORT = 22
        else:
            # QEMU's hard coded usermode router address
            self.HOST_ADDRESS = "10.0.2.2"
            self.GUEST_ADDRESS = "127.0.0.1"
            self.BRIDGE_HOSTA_PORT = network.find_free_port(address="127.0.0.1")
            self.BRIDGE_HOSTB_PORT = network.find_free_port(address="127.0.0.1")
            self.SSH_PORT = network.find_free_port(address="127.0.0.1")

        self.CLOUDINIT_HOME_PORT = network.find_free_port()

        # Find free port range
        base_port = 1024
        while True:
            base_port = network.find_free_port(start_port=base_port,
                                               address="127.0.0.1")
            if base_port == None:
                self.cancel("Failed to find a free port")
            for n in range(base_port, base_port +4):
                if not network.is_port_free(n, "127.0.0.1"):
                    base_port = n +1
                    break
            else:
                # for loop above didn't break
                break

        self.BASE_PORT = base_port

        # Disk images
        self.log.info("Downloading/preparing boot image")
        self.HOSTA_IMAGE = self.get_image(self.TMPA).path
        self.HOSTB_IMAGE = self.get_image(self.TMPB).path
        self.CLOUDINIT_ISO = os.path.join(self.TMPDIR, "cloudinit.iso")

        self.log.info("Preparing cloudinit image")
        try:
            cloudinit.iso(self.CLOUDINIT_ISO, self.name,
                          username="test", password="password",
                          phone_home_host=self.HOST_ADDRESS,
                          phone_home_port=self.CLOUDINIT_HOME_PORT)
        except Exception as e:
            self.cancel("Failed to prepare cloudinit image")

        self.QEMU_OPTIONS += " -cdrom %s" % self.CLOUDINIT_ISO

        # Network bridge
        if not self.BRIDGE_NAME:
            self.BRIDGE_PIDFILE = os.path.join(self.TMPDIR, "bridge.pid")
            self.run_command(("'%s' -pidfile '%s'"
                " -M none -display none -daemonize"
                " -netdev user,id=host,hostfwd=tcp:127.0.0.1:%s-:22"
                " -netdev socket,id=hosta,listen=127.0.0.1:%s"
                " -netdev socket,id=hostb,listen=127.0.0.1:%s"
                " -netdev hubport,id=hostport,hubid=0,netdev=host"
                " -netdev hubport,id=porta,hubid=0,netdev=hosta"
                " -netdev hubport,id=portb,hubid=0,netdev=hostb")
                % (self.QEMU_BINARY, self.BRIDGE_PIDFILE, self.SSH_PORT,
                   self.BRIDGE_HOSTA_PORT, self.BRIDGE_HOSTB_PORT), 0)

    def tearDown(self):
        try:
            pid = self.read_pidfile(self.BRIDGE_PIDFILE)
            if pid and self.check_pid(pid):
                os.kill(pid, signal.SIGKILL)
        except Exception as e:
            pass

        try:
            self.ra_stop(self.HOSTA)
        except Exception as e:
            pass

        try:
            self.ra_stop(self.HOSTB)
        except Exception as e:
            pass

        try:
            self.ssh_close()
        except Exception as e:
            pass

        for proc in self.traffic_procs:
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except Exception as e:
                pass

        shutil.rmtree(self.TMPDIR)

    def run_command(self, cmdline, expected_status, env=None):
        proc = subprocess.Popen(cmdline, shell=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT,
                                universal_newlines=True, env=env)
        stdout, stderr = proc.communicate()
        if proc.returncode != expected_status:
            self.fail("command \"%s\" failed with code %s:\n%s" \
                           % (cmdline, proc.returncode, stdout))

        return proc.returncode

    def cat_line(self, path):
        line=""
        try:
            fd = open(path, "r")
            line = str.strip(fd.readline())
            fd.close()
        except:
            pass
        return line

    def read_pidfile(self, pidfile):
        try:
            pid = int(self.cat_line(pidfile))
        except ValueError:
            return None
        else:
            return pid

    def check_pid(self, pid):
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        else:
            return True

    def ssh_open(self):
        self.ssh_conn = ssh.Session(self.GUEST_ADDRESS, self.SSH_PORT,
                                    user="test", password="password")
        self.ssh_conn.DEFAULT_OPTIONS += (("UserKnownHostsFile", "/dev/null"),)
        for i in range(10):
            try:
                if self.ssh_conn.connect():
                    return
            except Exception as e:
                pass
            time.sleep(4)
        self.fail("sshd timeout")

    def ssh_ping(self):
        if self.ssh_conn.cmd("echo ping").stdout != b"ping\n":
            self.fail("ssh ping failed")

    def ssh_close(self):
        self.ssh_conn.quit()

    def setup_base_env(self, host):
        PATH = os.getenv("PATH", "")
        env = { "PATH": "%s:%s" % (self.ADD_PATH, PATH),
                "HA_LOGFILE": self.RA_LOG,
                "OCF_RESOURCE_INSTANCE": "colo-test",
                "OCF_RESKEY_CRM_meta_clone_max": "2",
                "OCF_RESKEY_CRM_meta_notify": "true",
                "OCF_RESKEY_CRM_meta_timeout": "30000",
                "OCF_RESKEY_qemu_binary": self.QEMU_BINARY,
                "OCF_RESKEY_qemu_img_binary": self.QEMU_IMG_BINARY,
                "OCF_RESKEY_disk_size": str(self.IMAGE_SIZE),
                "OCF_RESKEY_checkpoint_interval": "10000",
                "OCF_RESKEY_base_port": str(self.BASE_PORT),
                "OCF_RESKEY_debug": "true",
                "OCF_RESKEY_dump_core": "true"}

        if host == self.HOSTA:
            env.update({"OCF_RESKEY_options":
                            ("%s -drive if=none,id=parent0,file='%s'")
                            % (self.QEMU_OPTIONS, self.HOSTA_IMAGE),
                        "OCF_RESKEY_active_hidden_dir": self.TMPA,
                        "OCF_RESKEY_listen_address": "127.0.0.1",
                        "OCF_RESKEY_log_dir": self.HOSTA_LOGDIR,
                        "OCF_RESKEY_CRM_meta_on_node": "127.0.0.1",
                        "HA_RSCTMP": self.TMPA,
                        "COLO_SMOKE_REMOTE_TMP": self.TMPB})

        else:
            env.update({"OCF_RESKEY_options":
                            ("%s -drive if=none,id=parent0,file='%s'")
                            % (self.QEMU_OPTIONS, self.HOSTB_IMAGE),
                        "OCF_RESKEY_active_hidden_dir": self.TMPB,
                        "OCF_RESKEY_listen_address": "127.0.0.2",
                        "OCF_RESKEY_log_dir": self.HOSTB_LOGDIR,
                        "OCF_RESKEY_CRM_meta_on_node": "127.0.0.2",
                        "HA_RSCTMP": self.TMPB,
                        "COLO_SMOKE_REMOTE_TMP": self.TMPA})

        if self.BRIDGE_NAME:
            env["OCF_RESKEY_options"] += \
                " -netdev bridge,id=hn0,br=%s,helper='%s'" \
                % (self.BRIDGE_NAME, self.BRIDGE_HELPER)
        else:
            if host == self.HOSTA:
                env["OCF_RESKEY_options"] += \
                    " -netdev socket,id=hn0,connect=127.0.0.1:%s" \
                    % self.BRIDGE_HOSTA_PORT
            else:
                env["OCF_RESKEY_options"] += \
                    " -netdev socket,id=hn0,connect=127.0.0.1:%s" \
                    % self.BRIDGE_HOSTB_PORT
        return env

    def ra_start(self, host):
        env = self.setup_base_env(host)
        self.run_command(self.RESOURCE_AGENT + " start", self.OCF_SUCCESS, env)

    def ra_stop(self, host):
        env = self.setup_base_env(host)
        self.run_command(self.RESOURCE_AGENT + " stop", self.OCF_SUCCESS, env)

    def ra_monitor(self, host, expected_status):
        env = self.setup_base_env(host)
        self.run_command(self.RESOURCE_AGENT + " monitor", expected_status, env)

    def ra_promote(self, host):
        env = self.setup_base_env(host)
        self.run_command(self.RESOURCE_AGENT + " promote", self.OCF_SUCCESS,env)

    def ra_notify_start(self, host):
        env = self.setup_base_env(host)

        env.update({"OCF_RESKEY_CRM_meta_notify_type": "post",
                    "OCF_RESKEY_CRM_meta_notify_operation": "start"})

        if host == self.HOSTA:
            env.update({"OCF_RESKEY_CRM_meta_notify_master_uname": "127.0.0.1",
                        "OCF_RESKEY_CRM_meta_notify_start_uname": "127.0.0.2"})
        else:
            env.update({"OCF_RESKEY_CRM_meta_notify_master_uname": "127.0.0.2",
                        "OCF_RESKEY_CRM_meta_notify_start_uname": "127.0.0.1"})

        self.run_command(self.RESOURCE_AGENT + " notify", self.OCF_SUCCESS, env)

    def ra_notify_stop(self, host):
        env = self.setup_base_env(host)

        env.update({"OCF_RESKEY_CRM_meta_notify_type": "pre",
                    "OCF_RESKEY_CRM_meta_notify_operation": "stop"})

        if host == self.HOSTA:
            env.update({"OCF_RESKEY_CRM_meta_notify_master_uname": "127.0.0.1",
                        "OCF_RESKEY_CRM_meta_notify_stop_uname": "127.0.0.2"})
        else:
            env.update({"OCF_RESKEY_CRM_meta_notify_master_uname": "127.0.0.2",
                        "OCF_RESKEY_CRM_meta_notify_stop_uname": "127.0.0.1"})

        self.run_command(self.RESOURCE_AGENT + " notify", self.OCF_SUCCESS, env)

    def get_pid(self, host):
        if host == self.HOSTA:
            return self.read_pidfile(os.path.join(self.TMPA,
                                                 "colo-test-qemu.pid"))
        else:
            return self.read_pidfile(os.path.join(self.TMPB,
                                                 "colo-test-qemu.pid"))

    def get_master_score(self, host):
        if host == self.HOSTA:
            return int(self.cat_line(os.path.join(self.TMPA, "master_score")))
        else:
            return int(self.cat_line(os.path.join(self.TMPB, "master_score")))

    def kill_qemu_pre(self, host):
        pid = self.get_pid(host)

        if pid and self.check_pid(pid):
            if self.hang_qemu:
                os.kill(pid, signal.SIGSTOP)
            else:
                os.kill(pid, signal.SIGKILL)
                while self.check_pid(pid):
                    time.sleep(1)

    def kill_qemu_post(self, host):
        pid = self.get_pid(host)

        if self.hang_qemu and pid and self.check_pid(pid):
            os.kill(pid, signal.SIGKILL)
            while self.check_pid(pid):
                time.sleep(1)

    def prepare_guest(self):
        pass

    def cycle_start(self, cycle):
        pass

    def active_section(self):
        return False

    def cycle_end(self, cycle):
        pass

    def _test_colo(self, loop=1):
        loop = max(loop, 1)
        self.log.info("Will put logs in %s" % self.outputdir)

        self.ra_stop(self.HOSTA)
        self.ra_stop(self.HOSTB)

        self.log.info("*** Startup ***")
        self.ra_start(self.HOSTA)
        self.ra_start(self.HOSTB)

        self.ra_monitor(self.HOSTA, self.OCF_SUCCESS)
        self.ra_monitor(self.HOSTB, self.OCF_SUCCESS)

        self.log.info("*** Promoting ***")
        self.ra_promote(self.HOSTA)
        cloudinit.wait_for_phone_home(("0.0.0.0", self.CLOUDINIT_HOME_PORT),
                                      self.name)
        self.ssh_open()
        self.prepare_guest()

        self.ra_notify_start(self.HOSTA)

        while self.get_master_score(self.HOSTB) != 100:
            self.ra_monitor(self.HOSTA, self.OCF_RUNNING_MASTER)
            self.ra_monitor(self.HOSTB, self.OCF_SUCCESS)
            time.sleep(1)

        self.log.info("*** Replication started ***")

        self.ssh_ping()

        primary = self.HOSTA
        secondary = self.HOSTB

        for n in range(loop):
            self.cycle_start(n)
            self.log.info("*** Secondary failover ***")
            self.kill_qemu_pre(primary)
            self.ra_notify_stop(secondary)
            self.ra_monitor(secondary, self.OCF_SUCCESS)
            self.ra_promote(secondary)
            self.ra_monitor(secondary, self.OCF_RUNNING_MASTER)
            self.kill_qemu_post(primary)

            self.ssh_ping()

            tmp = primary
            primary = secondary
            secondary = tmp

            self.log.info("*** Secondary continue replication ***")
            self.ra_start(secondary)
            self.ra_notify_start(primary)

            self.ssh_ping()

            # Wait for resync
            while self.get_master_score(secondary) != 100:
                self.ra_monitor(primary, self.OCF_RUNNING_MASTER)
                self.ra_monitor(secondary, self.OCF_SUCCESS)
                time.sleep(1)

            self.log.info("*** Replication started ***")

            self.ssh_ping()

            if self.active_section():
                break

            self.log.info("*** Primary failover ***")
            self.kill_qemu_pre(secondary)
            self.ra_monitor(primary, self.OCF_RUNNING_MASTER)
            self.ra_notify_stop(primary)
            self.ra_monitor(primary, self.OCF_RUNNING_MASTER)
            self.kill_qemu_post(secondary)

            self.ssh_ping()

            self.log.info("*** Primary continue replication ***")
            self.ra_start(secondary)
            self.ra_notify_start(primary)

            self.ssh_ping()

            # Wait for resync
            while self.get_master_score(secondary) != 100:
                self.ra_monitor(primary, self.OCF_RUNNING_MASTER)
                self.ra_monitor(secondary, self.OCF_SUCCESS)
                time.sleep(1)

            self.log.info("*** Replication started ***")

            self.ssh_ping()

            self.cycle_end(n)

        self.ssh_close()

        self.ra_stop(self.HOSTA)
        self.ra_stop(self.HOSTB)

        self.ra_monitor(self.HOSTA, self.OCF_NOT_RUNNING)
        self.ra_monitor(self.HOSTB, self.OCF_NOT_RUNNING)
        self.log.info("*** all ok ***")


class ColoQuickTest(ColoTest):
    """
    :avocado: tags=colo
    :avocado: tags=quick
    :avocado: tags=arch:x86_64
    """

    timeout = 300

    def cycle_end(self, cycle):
        self.log.info("Testing with peer qemu hanging")
        self.hang_qemu = True

    def test_quick(self):
        self.log.info("Testing with peer qemu crashing")
        self._test_colo(loop=2)


class ColoNetworkTest(ColoTest):

    def prepare_guest(self):
        self.ssh_conn.cmd("sudo -n dnf -q -y install %s;"
                          "sudo -n dnf -q -y install iperf3;"
                          "iperf3 -sD;" \
                          % self.RPMFUSION_URL)

    def cycle_start(self, cycle):
        args = [("", "client-to-server tcp"),
                ("-R", "server-to-client tcp"),
                ("-u", "client-to-server udp"),
                ("-uR", "server-to-client udp")]

        self.log.info("Testing iperf %s" % args[cycle % 4][1])
        self.traffic_procs.append(subprocess.Popen("while true; do iperf3 %s -t 300 -i 300 --connect-timeout 30000 -c %s; done" % (args[cycle % 4][0], self.GUEST_ADDRESS), shell=True, preexec_fn=os.setsid, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        time.sleep(5) # Wait for iperf to get up to speed

    def _cycle_end(self, cycle):
        pass

    def cycle_end(self, cycle):
        self._cycle_end(cycle)
        for proc in self.traffic_procs:
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except Exception as e:
                pass

class ColoRealNetworkTest(ColoNetworkTest):
    """
    :avocado: tags=colo
    :avocado: tags=slow
    :avocado: tags=network_test
    :avocado: tags=arch:x86_64
    """

    timeout = 1200

    def active_section(self):
        time.sleep(120)
        return False

    @skipUnless(iperf3_available(), "iperf3 not available")
    def test_network(self):
        if not self.BRIDGE_NAME:
            self.cancel("bridge options not given, will skip network test")
        self.log.info("Testing with peer qemu crashing and network load")
        self._test_colo(loop=4)

class ColoStressTest(ColoNetworkTest):
    """
    :avocado: tags=colo
    :avocado: tags=slow
    :avocado: tags=stress_test
    :avocado: tags=arch:x86_64
    """

    timeout = 1800

    def _cycle_end(self, cycle):
        if cycle == 7:
            self.log.info("Stress test with peer qemu hanging and network load")
            self.hang_qemu = True

    #TODO: only skip network load if iperf3 is missing
    @skipUnless(iperf3_available(), "iperf3 not available")
    def test_stress(self):
        if not self.BRIDGE_NAME:
            self.cancel("bridge options not given, will skip network test")
        self.log.info("Stress test with peer qemu crashing and network load")
        self._test_colo(loop=16)

class ColoPerformanceTest(ColoTest):
    """
    :avocado: tags=colo
    :avocado: tags=slow
    :avocado: tags=performance_test
    :avocado: tags=arch:x86_64
    """

    timeout = 600

    def prepare_guest(self):
        self.ssh_conn.cmd("sudo -n dnf -q -y install %s;"
                          "sudo -n dnf -q -y install iperf3 memtester;" \
                          % self.RPMFUSION_URL)

    def active_section(self):
        args = [("", "client-to-server-tcp"),
                ("-R", "server-to-client-tcp"),
                ("-u", "client-to-server-udp"),
                ("-uR", "server-to-client-udp")]

        self.ssh_conn.cmd("iperf3 -sD;"
                          "nohup sudo -n memtester 256 >/dev/null 2>&1 &")
        time.sleep(1)

        for test in args:
            self.log.info("Testing iperf %s" % test[1])
            output = os.path.join(self.outputdir, "iperf-" + test[1])
            for n in range(4):
                status = process.run("iperf3 %s -t 10 -i 10 -c %s >> '%s'" \
                                     % (test[0], self.GUEST_ADDRESS, output),
                                     shell=True)
        return True

    @skipUnless(iperf3_available(), "iperf3 not available")
    def test_performance(self):
        if not self.BRIDGE_NAME:
            self.cancel("bridge options not given, will skip performance test")
        self.log.info("Testing performance")
        self._test_colo()
