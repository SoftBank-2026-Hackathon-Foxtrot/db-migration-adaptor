import pytest

from db_operator.models import OperatorError
from db_operator.postgres import rewrite_route


def test_only_route_values_change_and_crlf_include_comments_are_preserved():
    before = (
        "; retain the exact document\r\n%include /etc/pgbouncer/auth.ini\r\n"
        "[databases]\r\n  appdb = host = 'old host'  pool_size=7 port=5432 "
        "auth_user=lookup password='can''t lose this' dbname=old_db   \r\n"
        "other = host=unchanged\r\n[pgbouncer]\r\npool_mode=transaction\r\n"
    )
    after = rewrite_route(before, "appdb", {"host": "new", "port": "5433", "dbname": "new_db"})
    assert after == before.replace("'old host'", "new").replace("port=5432", "port=5433").replace(
        "dbname=old_db", "dbname=new_db"
    )


def test_missing_route_parameters_are_appended_without_losing_options():
    assert (
        rewrite_route("[databases]\nappdb=pool_size=7  \n", "appdb", {"host": "new"})
        == "[databases]\nappdb=pool_size=7 host=new  \n"
    )


@pytest.mark.parametrize(
    "text",
    [
        "[databases]\nappdb=host=a host=b\n",
        "[databases]\nappdb=host=a\nappdb=host=b\n",
        "%include /etc/pgbouncer/routes.ini\n",
        "[databases]\nappdb=host='unfinished\n",
    ],
)
def test_ambiguous_or_unregistered_route_is_rejected_before_write(text):
    with pytest.raises(OperatorError, match="."):
        rewrite_route(text, "appdb", {"host": "new"})
