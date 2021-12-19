from EVENT_BAR_CANDIDATE_CHECK import StudyThreeBarsCandidates
from unittest import mock, TestCase
from testUtil import TestPublisher


class TestEventBarCandidateCheck(TestCase):

    def setUp(self):
        self.data = {
            'type': 'threebars',
            'symbol': 'FANG',
            'period': '5Min',
            'data': [
                {'t': 1639319700, 'c': 10.5, 'o': 10.1,
                    'h': 11.3, 'l': 9.05, 'v': 16200000.0},
                {'t': 1639319400, 'c': 10.0, 'o': 11.1,
                    'h': 11.3, 'l': 9.05, 'v': 16200000.0},
                {'t': 1639319100, 'c': 11.0, 'o': 10.6,
                    'h': 11.3, 'l': 9.05, 'v': 16200000.0},
                {'t': 1639318800, 'c': 10.5, 'o': 11.1,
                    'h': 11.3, 'l': 9.55, 'v': 16200000.0},
                {'t': 1639318500, 'c': 11.0, 'o': 11.1,
                    'h': 11.3, 'l': 9.05, 'v': 16200000.0},
                {'t': 1639318200, 'c': 11.0, 'o': 10.1,
                    'h': 11.3, 'l': 9.05, 'v': 16200000.0},
                {'t': 1639317900, 'c': 10.0, 'o': 11.1,
                    'h': 11.3, 'l': 9.05, 'v': 16200000.0},
                {'t': 1639317600, 'c': 11.0, 'o': 10.1,
                    'h': 11.3, 'l': 9.05, 'v': 10800000.0}
            ]
        }

    def tearDown(self):
        self.data = None

    def printOne(self, data):
        print(data)

    def test_filter_check_pass1(self):
        self.data['data'][0]['c'] = 10.5
        self.data['data'][1]['c'] = 11.0
        self.data['data'][2]['c'] = 11.0
        self.data['data'][3]['c'] = 10.0
        pubStack = TestPublisher(
            lambda package: self.asertEqual(self.data, package))
        pubScore = TestPublisher(
            lambda package: self.asertEqual(self.data, package))
        candidate = StudyThreeBarsCandidates(pubStack, pubScore)
        candidate.filterCheck(self.data)

    def test_filter_check_pass2(self):
        self.data['data'][0]['c'] = 10.5
        self.data['data'][1]['c'] = 11.0
        self.data['data'][2]['c'] = 10.0
        self.data['data'][3]['c'] = 10.0
        pubStack = TestPublisher(
            lambda package: self.asertEqual(self.data, package))
        pubScore = TestPublisher(
            lambda package: self.asertEqual(self.data, package))
        candidate = StudyThreeBarsCandidates(pubStack, pubScore)
        candidate.filterCheck(self.data)

    def test_filter_check_fail1(self):
        self.data['data'][0]['c'] = 10.0
        self.data['data'][1]['c'] = 11.0
        self.data['data'][2]['c'] = 10.0
        self.data['data'][3]['c'] = 10.0
        pubStack = TestPublisher(
            lambda package: self.asertNotEqual(self.data, package))
        pubScore = TestPublisher(
            lambda package: self.asertNotEqual(self.data, package))
        candidate = StudyThreeBarsCandidates(pubStack, pubScore)
        candidate.filterCheck(self.data)

    def test_filter_check_fail2(self):
        self.data['data'][0]['c'] = 10.0
        self.data['data'][1]['c'] = 10.0
        self.data['data'][2]['c'] = 10.0
        self.data['data'][3]['c'] = 10.0
        pubStack = TestPublisher(
            lambda package: self.asertNotEqual(self.data, package))
        pubScore = TestPublisher(
            lambda package: self.asertNotEqual(self.data, package))
        candidate = StudyThreeBarsCandidates(pubStack, pubScore)
        candidate.filterCheck(self.data)
