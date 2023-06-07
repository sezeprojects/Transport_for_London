from playground import get_bus_arrivals
import pytest


def test_if_passed_empty_url():
    url = ''
    with pytest.raises(Exception):
        get_bus_arrivals(url)

def test_success_status_code():
    url = "https://api.tfl.gov.uk/Stoppoint?lat=51.4929&lon=" \
      "0.053929&stoptypes=NaptanBusCoachStation," \
      "NaptanPublicBusCoachTram"
    status_code = get_bus_arrivals(url).status_code == 200

def test_raise_error_if_failed():
    url = 'https://api.tfl.gov.uyyyy'
    with pytest.raises(Exception):
        get_bus_arrivals(url)