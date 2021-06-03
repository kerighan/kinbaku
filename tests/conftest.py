import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--nodes",
        help="number of nodes",
        default=100,
        type=int
    )
    parser.addoption(
        "--degree",
        help="average node degree",
        default=10,
        type=int
    )


@pytest.fixture()
def N(request):
    return request.config.getoption("--nodes")


@pytest.fixture()
def M(request, N):
    return request.config.getoption("--degree") * N
