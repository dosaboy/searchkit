""" Searchkit search constraints unit tests. """
import os
import re
import tempfile
import shutil
import subprocess
import logging
from datetime import datetime
from unittest import mock
from unittest.mock import patch
from io import BytesIO

from searchkit.constraints import (
    FindTokenStatus,
    LogLine,
    LogFileDateSinceSeeker,
    MaxSearchableLineLengthReached,
    NoTimestampsFoundInFile,
    NoValidLinesFoundInFile,
    SavedFilePosition,
    SearchState,
    SearchConstraintSearchSince,
    TimestampMatcherBase,
)

from . import utils


class TimestampSimple(TimestampMatcherBase):

    @property
    def patterns(self):
        return [r'^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})[\sT]+'
                r'(?P<hours>\d{2}):(?P<minutes>\d{2}):(?P<seconds>\d+)']


LOGS_W_TS = """2022-01-01 00:00:00.00 L0
2022-01-01 01:00:00.00 L1
2022-01-02 00:00:00.00 L2
2022-01-02 01:00:00.00 L3
2022-01-03 00:00:00.00 L4"""

LOGS_W_TS_AND_UNMATCABLE_LINES = """blah 1
2022-01-01 00:00:00.00 L0
blah 2
blah 3
2022-01-01 01:00:00.00 L1
blah 4
2022-01-02 00:00:00.00 L2
blah 5
blah 6
blah 7
2022-01-02 01:00:00.00 L3
blah 8
2022-01-03 00:00:00.00 L4
blah 9
"""

# NOTE: a non-unicode char \xe2 has been inserted into the following logs
MYSQL_LOGS = b"""2019-04-04T14:47:33.199550Z mysqld_safe Logging to '/var/log/mysql/error.log'.
2019-04-04T14:47:33.243881Z mysqld_safe Starting mysqld daemon with databases from /var/lib/percona-xtradb-cluster
2019-04-04T14:47:33.259636Z mysqld_safe Skipping wsrep-recover for f29f7843-56e6-11e9-b1f5-7e15c4ed7ddc:2 pair
2019-04-04T14:47:33.261913Z mysqld_safe Assigning f29f7843-56e6-11e9-b1f5-7e15c4ed7ddc:2 to wsrep_start_position
2019-04-04T14:47:33.279756Z 0 [Warning] Changed limits: max_open_files: 5000 (requested 10005)
2019-04-04T14:47:33.279845Z 0 [Warning] Changed limits: table_open_cache: 1495 (requested 2048)
2019-04-04T14:47:33.478375Z 0 [Warning] TIMESTAMP with implicit DEFAULT value is deprecated. Please use --explicit_defaults_for_timestamp server option (see documentation for more details).
2019-04-04T14:47:33.480000Z 0 [Note] /usr/sbin/mysqld (mysqld 5.7.20-18-18-log) starting as process 126025 ...
2019-04-04T14:47:33.482384Z 0 [Note] WSREP: Setting wsrep_ready to false
2019-04-04T14:47:33.482398Z 0 [Note] WSREP: No pre-stored wsrep-start position found. Skipping position initialization.
2019-04-04T14:47:33.482402Z 0 [Note] WSREP: wsrep_load(): loading provider library '/usr/lib/galera3/libgalera_smm.so'
2019-04-04T14:47:33.484752Z 0 [Note] WSREP: wsrep_load(): Galera 3.24(r) by Codership Oy <info@codership.com> loaded successfully.
2019-04-04T14:47:33.484800Z 0 [Note] WSREP: CRC-32C: using hardware acceleration.
2019-04-04T14:47:33.485042Z 0 [Note] WSREP: Found saved state: f29f7843-56e6-11e9-b1f5-7e15c4ed7ddc:2, safe_to_bootstrap: 1
2019-04-04T14:47:33.486876Z 0 [Note] WSREP: Passing config to GCS: base_dir = /var/lib/percona-xtradb-cluster/; base_host = 10.160.0.153; base_port = 4567; cert.log_conflicts = no; debug = no; evs.auto_evict = 0; evs.delay_margin = PT1S; evs.delayed_keep_period = PT30S; evs.inactive_check_period = PT0.5S; evs.inactive_timeout = PT15S; evs.join_retrans_period = PT1S; evs.max_install_timeouts = 3; evs.send_window = 10; evs.stats_report_period = PT1M; evs.suspect_timeout = PT5S; evs.user_send_window = 4; evs.view_forget_timeout = PT24H; gcache.dir = /var/lib/percona-xtradb-cluster/; gcache.freeze_purge_at_seqno = -1; gcache.keep_pages_count = 0; gcache.keep_pages_size = 0; gcache.mem_size = 0; gcache.name = /var/lib/percona-xtradb-cluster//galera.cache; gcache.page_size = 128M; gcache.recover = no; gcache.size = 128M; gcomm.thread_prio = ; gcs.fc_debug = 0; gcs.fc_factor = 1; gcs.fc_limit = 100; gcs.fc_master_slave = no; gcs.max_packet_size = 64500; gcs.max_throttle = 0.25; gcs.recv_q_hard_limit = 9223372036854775807; gcs.recv_q_soft_limit = 0.25; gcs.sync_donor = no; gmcast.segment = 0; gmcast.version = 0; pc.announce_timeout = PT3S; pc.checksum = false; pc.ignore_quorum = false; pc.ignore_sb = false; pc.npvo = false; pc.recovery = 1; pc.version = 0; pc.wait_prim = true; pc.wait_prim_timeout = PT30S; pc.weight = 1; protonet.backend = asio; protonet.version = 0; repl.causal_read_timeout = PT30S; repl.commit_order = 3; repl.key_format = FLAT8; repl.max_ws_size = 2147483647; repl.proto_max = 7; socket.checksum = 2; socket.recv_buf_size = 212992; 
2019-04-04T14:47:33.517506Z 0 [Note] WSREP: GCache history reset: f29f7843-56e6-11e9-b1f5-7e15c4ed7ddc:0 -> f29f7843-56e6-11e9-b1f5-7e15c4ed7ddc:2
2019-04-04T14:47:33.517838Z 0 [Note] WSREP: Assign initial position for certification: 2, protocol version: -1
2019-04-04T14:47:33.517876Z 0 [Note] WSREP: Preparing to initiate SST/IST
2019-04-04T14:47:33.517887Z 0 [Note] WSREP: Starting replication
2019-04-04T14:47:33.517906Z 0 [Note] WSREP: Setting initial position to f29f7843-56e6-11e9-b1f5-7e15c4ed7ddc:2
2019-04-04T14:47:33.518229Z 0 [Note] WSREP: Using CRC-32C for message checksums.
2019-04-04T14:47:33.518374Z 0 [Note] WSREP: gcomm thread scheduling priority set to other:0 
2019-04-04T14:47:33.518535Z 0 [Warning] WSREP: Fail to access the file (/var/lib/percona-xtradb-cluster//gvwstate.dat) error (No such file or directory). It is possible if node is booting for first time or re-booting after a graceful shutdown
2019-04-04T14:47:33.518549Z 0 [Note] WSREP: Restoring primary-component from disk failed. Either node is booting for first time or re-booting after a graceful shutdown
2019-04-04T14:47:33.518960Z 0 [Note] WSREP: GMCast version 0
2019-04-04T14:47:33.519303Z 0 [Note] WSREP: (947dd30b, 'tcp://0.0.0.0:4567') listening at tcp://0.0.0.0:4567
2019-04-04T14:47:33.519324Z 0 [Note] WSREP: (947dd30b, 'tcp://0.0.0.0:4567') multicast: , ttl: 1
2019-04-04T14:47:33.520034Z 0 [Note] WSREP: EVS version 0
2019-04-04T14:47:33.520195Z 0 [Note] WSREP: gcomm: connecting to group 'juju_cluster', peer '10.160.0.148:'
2019-04-04T14:47:34.532443Z 0 [Note] WSREP: (947dd30b, 'tcp://0.0.0.0:4567') connection established to 0256ddd2 tcp://10.160.0.148:4567
2019-04-04T14:47:34.532579Z 0 [Note] WSREP: (947dd30b, 'tcp://0.0.0.0:4567') turning message relay requesting on, nonlive peers: 
2019-04-04T14:47:35.022049Z 0 [Note] WSREP: declaring 0256ddd2 at tcp://10.160.0.148:4567 stable
2019-04-04T14:47:35.022303Z 0 [Note] WSREP: Node 0256ddd2 state primary
2019-04-04T14:47:35.023075Z 0 [Note] WSREP: Current view of cluster as seen by this node
view (view_id(PRIM,0256ddd2,2)
memb {
	0256ddd2,0
	947dd30b,0
	}
joined {
	}
left {
	}
partitioned {
	}
)
2019-04-04T14:47:35.023099Z 0 [Note] WSREP: Save the discovered primary-component to disk
2019-04-04T14:47:35.521301Z 0 [Note] WSREP: gcomm: connected \xe2
2019-04-04T14:47:35.521465Z 0 [Note] WSREP: Shifting CLOSED -> OPEN (TO: 0)
2019-04-04T14:47:35.521724Z 0 [Note] WSREP: New COMPONENT: primary = yes, bootstrap = no, my_idx = 1, memb_num = 2
2019-04-04T14:47:35.521778Z 0 [Note] WSREP: STATE EXCHANGE: Waiting for state UUID.
2019-04-04T14:47:35.521831Z 0 [Note] WSREP: STATE EXCHANGE: sent state msg: 95636247-56e8-11e9-baaa-cb92a57ee4d8
2019-04-04T14:47:35.521848Z 0 [Note] WSREP: STATE EXCHANGE: got state msg: 95636247-56e8-11e9-baaa-cb92a57ee4d8 from 0 (juju-0aa49a-7-lxd-7)
2019-04-04T14:47:35.521926Z 0 [Note] WSREP: Waiting for SST/IST to complete.
2019-04-04T14:47:35.522184Z 0 [Warning] WSREP: last inactive check more than PT1.5S (3*evs.inactive_check_period) ago (PT2.00221S), skipping check
2019-04-04T14:47:35.522333Z 0 [Note] WSREP: STATE EXCHANGE: got state msg: 95636247-56e8-11e9-baaa-cb92a57ee4d8 from 1 (juju-0aa49a-8-lxd-7)
2019-04-04T14:47:35.522352Z 0 [Note] WSREP: Quorum results:
	version    = 4,
	component  = PRIMARY,
	conf_id    = 1,
	members    = 1/2 (primary/total),
	act_id     = 2,
	last_appl. = -1,
	protocols  = 0/7/3 (gcs/repl/appl),
	group UUID = f020c044-56e4-11e9-9652-aad4a917a89c
2019-04-04T14:47:35.522366Z 0 [Note] WSREP: Flow-control interval: [141, 141]
2019-04-04T14:47:35.522376Z 0 [Note] WSREP: Trying to continue unpaused monitor
2019-04-04T14:47:35.522386Z 0 [Note] WSREP: Shifting OPEN -> PRIMARY (TO: 2)
2019-04-04T14:47:35.522479Z 2 [Note] WSREP: State transfer required: """ # noqa, pylint: disable=all


class TestSearchKitBase(utils.BaseTestCase):

    def get_date(self, date):
        cmd = ["date", "--utc", "--date={}".format(date), "+%Y-%m-%d %H:%M:%S"]
        out = subprocess.check_output(cmd)
        out = re.compile(r"\s+").sub(' ', out.decode('UTF-8')).strip()
        return out

    def setUp(self):
        super().setUp()
        self.current_date = self.get_date("Thu Feb 10 16:19:17 UTC 2022")
        self.data_root = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.data_root)
        super().tearDown()


class TestSearchConstraints(TestSearchKitBase):

    @utils.create_files({'f1': LOGS_W_TS})
    def test_binary_search_1(self):
        self.current_date = self.get_date('Tue Jan 03 00:00:01 UTC 2022')
        _file = os.path.join(self.data_root, 'f1')
        c = SearchConstraintSearchSince(current_date=self.current_date,
                                        ts_matcher_cls=TimestampSimple, days=7)
        with open(_file, 'rb') as fd:
            self.assertEqual(c.apply_to_file(fd), 0)

        stats = {'line': {'fail': 0, 'pass': 0}, 'lines_searched': 2}
        self.assertEqual(c.stats(), stats)

    @utils.create_files({'f1': LOGS_W_TS})
    def test_binary_search_2(self):
        self.current_date = self.get_date('Tue Jan 03 00:00:01 UTC 2022')
        _file = os.path.join(self.data_root, 'f1')

        c = SearchConstraintSearchSince(current_date=self.current_date,
                                        ts_matcher_cls=TimestampSimple, days=7)
        with open(_file, 'w') as fd:
            fd.write('somejunk\n' + LOGS_W_TS)

        with open(_file, 'rb') as fd:
            self.assertEqual(c.apply_to_file(fd), 9)

        stats = {'line': {'fail': 0, 'pass': 0}, 'lines_searched': 19}
        self.assertEqual(c.stats(), stats)

    @utils.create_files({'f1': LOGS_W_TS})
    def test_binary_search_3(self):
        self.current_date = self.get_date('Tue Jan 03 00:00:01 UTC 2022')
        _file = os.path.join(self.data_root, 'f1')
        c = SearchConstraintSearchSince(current_date=self.current_date,
                                        ts_matcher_cls=TimestampSimple, days=7)
        with open(_file, 'w') as fd:
            fd.write('somejunk\n' * 499 + LOGS_W_TS)

        with open(_file, 'rb') as fd:
            offset = c.apply_to_file(fd)
            self.assertEqual(offset, 4491)

        stats = {'line': {'fail': 0, 'pass': 0}, 'lines_searched': 6515}
        self.assertEqual(c.stats(), stats)

    @utils.create_files({'f1': LOGS_W_TS})
    def test_binary_search_4(self):
        self.current_date = self.get_date('Tue Jan 03 00:00:01 UTC 2022')
        _file = os.path.join(self.data_root, 'f1')
        c = SearchConstraintSearchSince(current_date=self.current_date,
                                        ts_matcher_cls=TimestampSimple, days=7)
        with open(_file, 'w') as fd:
            fd.write('somejunk\n' * 501 + LOGS_W_TS)

        with open(_file, 'rb') as fd:
            offset = c.apply_to_file(fd)
            self.assertEqual(offset, 0)

        stats = {'line': {'fail': 0, 'pass': 0}, 'lines_searched': 0}
        self.assertEqual(c.stats(), stats)

    @patch.object(LogFileDateSinceSeeker, 'run',
                  side_effect=MaxSearchableLineLengthReached)
    @patch.object(logging.Logger, "warning")
    @utils.create_files({'f1': LOGS_W_TS})
    def test_apply_to_file_throw_max_line_len_err(self, mock_log, mock_lfdss):
        self.current_date = self.get_date('Tue Jan 03 00:00:01 UTC 2022')
        _file = os.path.join(self.data_root, 'f1')
        c = SearchConstraintSearchSince(current_date=self.current_date,
                                        ts_matcher_cls=TimestampSimple, days=7)
        with open(_file, 'rb') as fd:
            c.apply_to_file(fd)
            args, kwargs = mock_log.call_args
            self.assertTrue("c:%s exceeded allowed line length search limit "
                            "before finding line feed: %s"
                            " - moving to EOF to skip searching this file"
                            in args)


class TestSearchState(TestSearchKitBase):

    def test_construct_found(self):
        uut = SearchState(FindTokenStatus.FOUND, 15)
        self.assertEqual(uut.status, FindTokenStatus.FOUND)
        self.assertEqual(uut.offset, 15)

    def test_construct_reached_eof(self):
        uut = SearchState(FindTokenStatus.REACHED_EOF, 15)
        self.assertEqual(uut.status, FindTokenStatus.REACHED_EOF)
        self.assertEqual(uut.offset, 15)


class TestSavedFilePosition(TestSearchKitBase):

    def test_peek(self):
        mock_file = mock.MagicMock()
        mock_file.tell.return_value = 0xBADC0DE
        with SavedFilePosition(mock_file):
            pass
        mock_file.seek.assert_called_once_with(0xBADC0DE)


class TestLogLine(TestSearchKitBase):

    def test_construct(self):
        mock_file = mock.MagicMock()
        mock_file.read.return_value = "01.01.1970 thisisaline"
        mock_constraint = mock.MagicMock()
        mock_constraint.extracted_datetime.return_value = datetime(1970, 1, 1)
        mock_lslf = mock.MagicMock()
        mock_lslf.offset = 5
        mock_lslf.status = FindTokenStatus.FOUND
        mock_lelf = mock.MagicMock()
        mock_lelf.offset = 10
        mock_lelf.status = FindTokenStatus.FOUND
        uut = LogLine(mock_file, mock_constraint, mock_lslf, mock_lelf)

        self.assertEqual(uut.start_lf.offset, 5)
        self.assertEqual(uut.end_lf.offset, 10)
        self.assertEqual(uut.start_offset, 6)
        self.assertEqual(uut.end_offset, 9)
        self.assertEqual(uut.text, "01.01.1970 thisisaline")
        self.assertEqual(uut.date, datetime(1970, 1, 1))

        mock_lslf.status = FindTokenStatus.REACHED_EOF
        mock_lelf.status = FindTokenStatus.REACHED_EOF

        self.assertEqual(uut.start_offset, 5)
        self.assertEqual(uut.end_offset, 10)


class TestLogFileDateSinceSeeker(TestSearchKitBase):

    def setUp(self):
        super().setUp()
        self.bio = BytesIO(MYSQL_LOGS)
        self.mock_file = mock.MagicMock()
        self.mock_file.read.side_effect = lambda amount: self.bio.read(amount)
        self.mock_file.seek.side_effect = lambda off, wh = 0: self.bio.seek(
            off, wh)
        self.mock_file.tell.side_effect = lambda: self.bio.tell()
        self.constraint = SearchConstraintSearchSince(
            current_date=self.get_date('Tue Apr 04 14:40:01 UTC 2019'),
            ts_matcher_cls=TimestampSimple, days=7)
        self.mock_constraint = mock.MagicMock()
        self.mock_constraint.extracted_datetime.return_value = datetime(
            2019, 4, 4, 14, 47, 33)
        self.max_line_length = LogFileDateSinceSeeker.MAX_SEEK_HORIZON_EXPAND
        self.max_line_length *= LogFileDateSinceSeeker.SEEK_HORIZON

    def test_construct(self):
        uut = LogFileDateSinceSeeker(
            self.mock_file,
            self.mock_constraint)
        self.assertEqual(uut.file, self.mock_file)
        self.assertEqual(uut.constraint, self.mock_constraint)
        self.assertEqual(uut.line_info, None)
        self.assertEqual(uut.found_any_date, False)

    def test_find_token_reverse(self):
        uut = LogFileDateSinceSeeker(
            self.mock_file,
            self.mock_constraint)
        # Expectation: there is one LF between [0,100], status should be FOUND
        # and offset should be `78`
        result = uut.find_token_reverse(100)
        self.assertEqual(result.status, FindTokenStatus.FOUND)
        self.assertEqual(result.offset, 78)

    def test_find_token_reverse_eof(self):
        uut = LogFileDateSinceSeeker(
            self.mock_file,
            self.mock_constraint)
        # Expectation: no LF between [0,77], status should be REACHED_EOF
        result = uut.find_token_reverse(77)
        self.assertEqual(result.status, FindTokenStatus.REACHED_EOF)
        self.assertEqual(result.offset, 0)

    def test_find_token_reverse_fail(self):
        self.mock_file.read.side_effect = lambda n: bytes(('A' * n).encode())
        uut = LogFileDateSinceSeeker(self.mock_file, self.mock_constraint)
        # Expectation: find_token_reverse should give up the search and
        # status should be `failed`
        with self.assertRaises(MaxSearchableLineLengthReached):
            uut.find_token_reverse(self.max_line_length + 257)

    def test_find_token(self):
        uut = LogFileDateSinceSeeker(self.mock_file, self.mock_constraint)
        # Expectation: there is one LF between [0,100], status should be FOUND
        # and offset should be `78`
        result = uut.find_token(78)
        self.assertEqual(result.status, FindTokenStatus.FOUND)
        self.assertEqual(result.offset, 78)

    def test_find_token_eof(self):
        uut = LogFileDateSinceSeeker(self.mock_file, self.mock_constraint)
        # Expectation: there is one LF between [0,100], status should be FOUND
        # and offset should be `78`
        result = uut.find_token(6896)
        self.assertEqual(result.status, FindTokenStatus.REACHED_EOF)
        self.assertEqual(result.offset, 6909)

    def test_find_token_fail(self):
        self.mock_file.read.side_effect = lambda n: bytes(('A' * n).encode())
        uut = LogFileDateSinceSeeker(self.mock_file, self.mock_constraint)
        # Expectation: there is one LF between [0,100], status should be FOUND
        # and offset should be `78`
        with self.assertRaises(MaxSearchableLineLengthReached):
            uut.find_token(100000)

    def test_try_find_line_slf_is_eof(self):
        uut = LogFileDateSinceSeeker(self.mock_file, self.mock_constraint)
        result = uut.try_find_line(74)
        self.assertEqual(result.start_lf.status, FindTokenStatus.REACHED_EOF)
        self.assertEqual(result.start_lf.offset, 0)
        self.assertEqual(result.end_lf.status, FindTokenStatus.FOUND)
        self.assertEqual(result.end_lf.offset, 78)
        self.assertEqual(result.start_offset, 0)
        self.assertEqual(result.end_offset, 77)
        self.assertEqual(result.text,
                         b"2019-04-04T14:47:33.199550Z mysqld_safe"
                         b" Logging to '/var/log/mysql/error.log'.")
        self.assertEqual(result.date, datetime(2019, 4, 4, 14, 47, 33))

    def test_try_find_line_elf_is_eof(self):
        uut = LogFileDateSinceSeeker(self.mock_file, self.mock_constraint)
        result = uut.try_find_line(6896)
        self.assertEqual(result.start_lf.status, FindTokenStatus.FOUND)
        self.assertEqual(result.start_lf.offset, 6839)
        self.assertEqual(result.end_lf.status, FindTokenStatus.REACHED_EOF)
        self.assertEqual(result.end_lf.offset, 6909)
        self.assertEqual(result.start_offset, 6840)
        self.assertEqual(result.end_offset, 6909)
        self.assertEqual(result.text,
                         b"2019-04-04T14:47:35.522479Z 2 [Note] WSREP:"
                         b" State transfer required: ")
        self.assertEqual(result.date, datetime(2019, 4, 4, 14, 47, 33))

    def test_try_find_line_both_slf_elf_eof(self):
        self.bio = BytesIO(b"thisisaline")
        uut = LogFileDateSinceSeeker(self.mock_file, self.mock_constraint)
        result = uut.try_find_line(4)
        self.assertEqual(result.start_lf.status, FindTokenStatus.REACHED_EOF)
        self.assertEqual(result.start_lf.offset, 0)
        self.assertEqual(result.end_lf.status, FindTokenStatus.REACHED_EOF)
        self.assertEqual(result.end_lf.offset, 11)
        self.assertEqual(result.start_offset, 0)
        self.assertEqual(result.end_offset, 11)
        self.assertEqual(result.text, b"thisisaline")
        self.assertEqual(result.date, datetime(2019, 4, 4, 14, 47, 33))

    def test_try_find_line_slf_elf_exists(self):
        uut = LogFileDateSinceSeeker(self.mock_file, self.mock_constraint)
        result = uut.try_find_line(83)
        self.assertEqual(result.start_lf.status, FindTokenStatus.FOUND)
        self.assertEqual(result.start_lf.offset, 78)
        self.assertEqual(result.end_lf.status, FindTokenStatus.FOUND)
        self.assertEqual(result.end_lf.offset, 193)
        self.assertEqual(result.start_offset, 79)
        self.assertEqual(result.end_offset, 192)
        self.assertEqual(result.text,
                         b"2019-04-04T14:47:33.243881Z mysqld_safe"
                         b" Starting mysqld daemon with databases from"
                         b" /var/lib/percona-xtradb-cluster")
        self.assertEqual(result.date, datetime(2019, 4, 4, 14, 47, 33))

    def test_try_find_line_elf_failed(self):
        self.mock_file.read.side_effect = lambda n: bytes(('A' * n).encode())
        uut = LogFileDateSinceSeeker(self.mock_file, self.mock_constraint)
        with self.assertRaises(MaxSearchableLineLengthReached) as rexc:
            uut.try_find_line(83)
            self.assertEqual(str(rexc.exception),
                             "Could not find ending line feed offset at "
                             "epicenter 83")

    def test_try_find_line_slf_failed(self):
        contents = ('A' * ((self.max_line_length * 2) - 1)) + '\n'
        self.bio = BytesIO(bytes(contents.encode()))
        uut = LogFileDateSinceSeeker(self.mock_file, self.mock_constraint)
        with self.assertRaises(MaxSearchableLineLengthReached) as rexc:
            uut.try_find_line(self.max_line_length)
            self.assertEqual(str(rexc.exception),
                             "Could not find start line feed offset at "
                             "epicenter 1048576")

    def test_try_find_line_w_constraint(self):
        uut = LogFileDateSinceSeeker(self.mock_file, self.constraint)
        result = uut.try_find_line(83)
        # 2019-04-04T14:47:33.199550Z
        self.assertEqual(result.date, datetime(2019, 4, 4, 14, 47, 33))

        result = uut.try_find_line(5000)
        self.assertEqual(result.date, datetime(2019, 4, 4, 14, 47, 35))

    def test_try_find_line_verify_offsets(self):
        uut = LogFileDateSinceSeeker(self.mock_file, self.constraint)
        seen_lines = set()
        expected_offsets = set(
            [(0, 77), (79, 192), (194, 303), (305, 416), (418, 511),
             (513, 607), (609, 797), (799, 908), (910, 981), (983, 1101),
             (1103, 1220), (1222, 1351), (1353, 1433), (1435, 1557),
             (1559, 3125), (3127, 3272), (3274, 3383), (3385, 3457),
             (3459, 3522), (3524, 3633), (3635, 3714), (3716, 3807),
             (3809, 4050), (4052, 4218), (4220, 4279), (4281, 4388),
             (4390, 4485), (4487, 4543), (4545, 4651), (4653, 4787),
             (4789, 4917), (4919, 5014), (5016, 5086), (5088, 5175),
             (5177, 5206), (5208, 5213), (5215, 5225), (5227, 5237),
             (5239, 5240), (5242, 5249), (5251, 5252), (5254, 5259),
             (5261, 5262), (5264, 5276), (5278, 5279), (5281, 5281),
             (5283, 5371), (5373, 5434), (5436, 5510), (5512, 5625),
             (5627, 5709), (5711, 5822), (5824, 5963), (5965, 6040),
             (6042, 6187), (6189, 6328), (6330, 6388), (6390, 6405),
             (6407, 6428), (6430, 6445), (6447, 6480), (6482, 6497),
             (6499, 6515), (6517, 6552), (6554, 6603), (6605, 6681),
             (6683, 6761), (6763, 6838), (6840, 6909)]
        )

        for i in range(0, len(MYSQL_LOGS)):
            result = uut.try_find_line(i)
            self.assertNotEqual(result, None)
            self.assertNotEqual(result.text, None)
            self.assertGreater(len(result.text), 0)
            self.assertTrue(result.date or "2019" not in result.text.decode())
            self.assertGreaterEqual(result.start_offset, 0)
            self.assertLessEqual(result.start_offset, len(MYSQL_LOGS))
            self.assertGreaterEqual(result.end_offset, 0)
            self.assertLessEqual(result.end_offset, len(MYSQL_LOGS))
            self.assertGreaterEqual(result.start_offset,
                                    result.start_lf.offset)
            self.assertLessEqual(result.start_offset,
                                 result.start_lf.offset + 1)
            self.assertGreaterEqual(result.end_offset,
                                    result.end_lf.offset - 1)
            self.assertLessEqual(result.end_offset,
                                 result.end_lf.offset)
            seen_lines.add((result.start_offset, result.end_offset))
        self.assertEqual(len(seen_lines), 69)
        self.assertEqual(len(seen_lines), len(expected_offsets))
        self.assertEqual(seen_lines, expected_offsets)

    def test_try_find_line_with_date(self):
        uut = LogFileDateSinceSeeker(self.mock_file, self.constraint)
        seen_lines = set()
        expected_offsets = set(
            [
                (0, 77), (79, 192), (194, 303), (305, 416), (418, 511),
                (513, 607), (609, 797), (799, 908), (910, 981), (983, 1101),
                (1103, 1220), (1222, 1351), (1353, 1433), (1435, 1557),
                (1559, 3125), (3127, 3272), (3274, 3383), (3385, 3457),
                (3459, 3522), (3524, 3633), (3635, 3714), (3716, 3807),
                (3809, 4050), (4052, 4218), (4220, 4279), (4281, 4388),
                (4390, 4485), (4487, 4543), (4545, 4651), (4653, 4787),
                (4789, 4917), (4919, 5014), (5016, 5086), (5088, 5175),
                (5283, 5371), (5373, 5434), (5436, 5510), (5512, 5625),
                (5627, 5709), (5711, 5822), (5824, 5963), (5965, 6040),
                (6042, 6187), (6189, 6328), (6330, 6388), (6605, 6681),
                (6683, 6761), (6763, 6838), (6840, 6909)
            ]
        )
        for i in range(0, len(MYSQL_LOGS)):
            result = uut.try_find_line_with_date(i)
            self.assertNotEqual(result, None)
            self.assertNotEqual(result.text, None)
            self.assertGreater(len(result.text), 0)
            self.assertNotEqual(result.date, None)
            self.assertGreaterEqual(result.start_offset, 0)
            self.assertLessEqual(result.start_offset, len(MYSQL_LOGS))
            self.assertGreaterEqual(result.end_offset, 0)
            self.assertLessEqual(result.end_offset, len(MYSQL_LOGS))
            self.assertGreaterEqual(result.start_offset,
                                    result.start_lf.offset)
            self.assertLessEqual(result.start_offset,
                                 result.start_lf.offset + 1)
            self.assertGreaterEqual(result.end_offset,
                                    result.end_lf.offset - 1)
            self.assertLessEqual(result.end_offset,
                                 result.end_lf.offset)
            seen_lines.add((result.start_offset, result.end_offset))

        # We expect here to see 49 seen lines as the log file contains
        # log lines without date. The algorithm should retrieve the nearest
        # log line with date in that case.
        self.assertEqual(len(seen_lines), 49)
        self.assertEqual(len(seen_lines), len(expected_offsets))
        self.assertEqual(seen_lines, expected_offsets)

    def test_try_find_line_with_date_fallback_backwards(self):
        uut = LogFileDateSinceSeeker(self.mock_file, self.constraint)
        for offset in [5181, 5212, 5248, 5258, 5275]:
            result = uut.try_find_line_with_date(offset, forwards=False)
            self.assertNotEqual(result, None)
            self.assertNotEqual(result.date, None)
            self.assertEqual(result.date, datetime(2019, 4, 4, 14, 47, 35))
            self.assertEqual(result.start_offset, 5088)
            self.assertEqual(result.end_offset, 5175)

    def test_try_find_line_with_date_fallback_forwards(self):
        uut = LogFileDateSinceSeeker(self.mock_file, self.constraint)
        for offset in [5181, 5212, 5248, 5258, 5275]:
            result = uut.try_find_line_with_date(offset, forwards=True)
            self.assertNotEqual(result, None)
            self.assertNotEqual(result.date, None)
            self.assertEqual(result.date, datetime(2019, 4, 4, 14, 47, 35))
            self.assertEqual(result.start_offset, 5283)
            self.assertEqual(result.end_offset, 5371)

    def test_getitem(self):
        uut = LogFileDateSinceSeeker(self.mock_file, self.constraint)
        for offset in [5181, 5212, 5248, 5258, 5275]:
            result = uut[offset]
            self.assertNotEqual(uut.line_info, None)
            self.assertNotEqual(uut.line_info.date, None)
            self.assertEqual(uut.line_info.date, result)
            self.assertEqual(result, datetime(2019, 4, 4, 14, 47, 35))
            self.assertEqual(uut.line_info.start_offset, 5088)
            self.assertEqual(uut.line_info.end_offset, 5175)

    def test_getitem_forwards(self):
        # Cut MYSQL_LOGS in such a way that there's no backwards logs
        # so the algorithm have to fallback to forwards search.
        self.bio = BytesIO(MYSQL_LOGS[5177:])
        uut = LogFileDateSinceSeeker(self.mock_file, self.constraint)
        for offset in [1, 31, 65, 77, 87]:
            result = uut[offset]
            self.assertNotEqual(uut.line_info, None)
            self.assertNotEqual(uut.line_info.date, None)
            self.assertEqual(uut.line_info.date, result)
            self.assertEqual(result, datetime(2019, 4, 4, 14, 47, 35))
            self.assertEqual(uut.line_info.start_offset, 106)
            self.assertEqual(uut.line_info.end_offset, 194)

    def test_getitem_backwards(self):
        # Cut MYSQL_LOGS in such a way that there's no forwards logs.
        self.bio = BytesIO(MYSQL_LOGS[5016:5282])
        uut = LogFileDateSinceSeeker(self.mock_file, self.constraint)
        for offset in [192, 226]:
            result = uut[offset]
            self.assertNotEqual(uut.line_info, None)
            self.assertNotEqual(uut.line_info.date, None)
            self.assertEqual(uut.line_info.date, result)
            self.assertEqual(result, datetime(2019, 4, 4, 14, 47, 35))
            self.assertEqual(uut.line_info.start_offset, 72)
            self.assertEqual(uut.line_info.end_offset, 159)

    def test_getitem_all(self):
        uut = LogFileDateSinceSeeker(self.mock_file, self.constraint)
        for i in range(0, len(MYSQL_LOGS)):
            result = uut.try_find_line_with_date(i)
            self.assertNotEqual(result, None)
            self.assertNotEqual(result.text, None)
            self.assertGreater(len(result.text), 0)
            self.assertNotEqual(result.date, None)
            self.assertGreaterEqual(result.start_offset, 0)
            self.assertLessEqual(result.start_offset, len(MYSQL_LOGS))
            self.assertGreaterEqual(result.end_offset, 0)
            self.assertLessEqual(result.end_offset, len(MYSQL_LOGS))
            self.assertGreaterEqual(result.start_offset,
                                    result.start_lf.offset)
            self.assertLessEqual(result.start_offset,
                                 result.start_lf.offset + 1)
            self.assertGreaterEqual(result.end_offset,
                                    result.end_lf.offset - 1)
            self.assertLessEqual(result.end_offset,
                                 result.end_lf.offset)

    def test_run_1(self):
        self.constraint = SearchConstraintSearchSince(
            current_date=self.get_date('Tue Apr 11 14:47:33 UTC 2019'),
            ts_matcher_cls=TimestampSimple, days=7)
        uut = LogFileDateSinceSeeker(self.mock_file, self.constraint)
        result = uut.run()
        self.assertEqual(result, 0)

    def test_run_2(self):
        self.constraint = SearchConstraintSearchSince(
            current_date=self.get_date('Tue Apr 11 14:47:34 UTC 2019'),
            ts_matcher_cls=TimestampSimple, days=7)
        uut = LogFileDateSinceSeeker(self.mock_file, self.constraint)
        result = uut.run()
        self.assertEqual(result, 4653)

    def test_run_3(self):
        self.constraint = SearchConstraintSearchSince(
            current_date=self.get_date('Tue Apr 11 14:47:35 UTC 2019'),
            ts_matcher_cls=TimestampSimple, days=7)
        uut = LogFileDateSinceSeeker(self.mock_file, self.constraint)
        result = uut.run()
        self.assertEqual(result, 4919)

    def test_run_4(self):
        self.constraint = SearchConstraintSearchSince(
            current_date=self.get_date('Tue Apr 11 14:47:35 UTC 2019'),
            ts_matcher_cls=TimestampSimple, days=7)
        uut = LogFileDateSinceSeeker(self.mock_file, self.constraint)
        result = uut.run()
        self.assertEqual(result, 4919)

    def test_run_before(self):
        self.constraint = SearchConstraintSearchSince(
            current_date=self.get_date('Tue Apr 11 14:47:32 UTC 2019'),
            ts_matcher_cls=TimestampSimple, days=7)
        uut = LogFileDateSinceSeeker(self.mock_file, self.constraint)
        result = uut.run()
        self.assertEqual(result, 0)

    def test_run_no_such_date(self):
        self.constraint = SearchConstraintSearchSince(
            current_date=self.get_date('Tue Apr 11 14:47:36 UTC 2019'),
            ts_matcher_cls=TimestampSimple, days=7)
        uut = LogFileDateSinceSeeker(self.mock_file, self.constraint)
        with self.assertRaises(NoValidLinesFoundInFile):
            uut.run()

    def test_run_no_date_found(self):
        self.bio = BytesIO(b"nodatewhatsoever")
        self.constraint = SearchConstraintSearchSince(
            current_date=self.get_date('Tue Apr 11 14:47:36 UTC 2019'),
            ts_matcher_cls=TimestampSimple, days=7)
        uut = LogFileDateSinceSeeker(self.mock_file, self.constraint)
        with self.assertRaises(NoTimestampsFoundInFile):
            uut.run()
