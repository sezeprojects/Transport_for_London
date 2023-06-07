from unittest.mock import patch
import pytest
from playground import get_data, stream_data

def test_if_passed_empty_url():
    url = ''
    with pytest.raises(Exception):
        get_data(url)

def test_success_status_code():
    url = "https://api.tfl.gov.uk/Stoppoint?lat=51.4929&lon=" \
      "0.053929&stoptypes=NaptanBusCoachStation," \
      "NaptanPublicBusCoachTram"
    status_code = get_data(url).status_code == 200

def test_raise_error_if_failed():
    url = 'https://api.tfl.gov.uyyyy'
    with pytest.raises(Exception):
        get_data(url)

# @patch('kafka.KafkaProducer')
# def test_produce_message(KafkaProducerMock):
#     stream_data('topic', 'data')
#     KafkaProducerMock.return_value.send.assert_called()