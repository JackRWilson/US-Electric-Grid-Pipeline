import logging
from urllib.parse import quote

from dash import Input, Output, State, dcc, html, no_update
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from styles import (
    DASH_LOADING_CONTAINER_STYLE,
    DASH_LOADING_HEADER_STYLE,
    DASH_LOADING_STYLE,
)
from data import ENGINE
from init_app import app
from layout import layout

logger = logging.getLogger(__name__)
ETL_REFRESH_POLL_INTERVAL = 60_000


def dashboard_data_ready():
    try:
        with ENGINE.connect() as connection:
            return bool(
                connection.execute(
                    text(
                        "select exists ("
                        "select 1 from electricity.etl_watermark "
                        "where source = 'static'"
                        ")"
                    )
                ).scalar_one()
            )
    except SQLAlchemyError as error:
        logger.info(
            "Dashboard is waiting for the database and initial ETL run: %s",
            error,
        )
        return False


def latest_etl_completion():
    try:
        with ENGINE.connect() as connection:
            completed = connection.execute(
                text(
                    "select last_period from electricity.etl_watermark "
                    "where source = 'etl'"
                )
            ).scalar_one_or_none()
        return completed.isoformat() if completed is not None else None
    except SQLAlchemyError as error:
        logger.info("Could not check the latest ETL completion: %s", error)
        return None


def loading_layout():
    return html.Main(
        [
            html.H1("Dashboard is getting ready", style=DASH_LOADING_HEADER_STYLE),
            html.P(
                "The database is starting or the first data load is still running."
                "This page will refresh automatically when the dashboard is ready.",
                style=DASH_LOADING_STYLE,
            ),
            dcc.Interval(id="startup-poll", interval=15000, n_intervals=0),
            dcc.Location(id="startup-refresh", refresh=True),
        ],
        style=DASH_LOADING_CONTAINER_STYLE,
    )


def dashboard_layout():
    if not dashboard_data_ready():
        return loading_layout()
    return html.Div(
        [
            dcc.Location(id="etl-refresh-location", refresh=True),
            dcc.Store(id="etl-last-completion", data=latest_etl_completion()),
            dcc.Interval(
                id="etl-refresh-poll",
                interval=ETL_REFRESH_POLL_INTERVAL,
                n_intervals=0,
            ),
            layout(),
        ]
    )


app.config.suppress_callback_exceptions = True
app.layout = dashboard_layout


@app.callback(
    Output("startup-refresh", "href"),
    Input("startup-poll", "n_intervals"),
)
def refresh_when_ready(_interval):
    return "/" if dashboard_data_ready() else no_update


@app.callback(
    Output("etl-refresh-location", "href"),
    Input("etl-refresh-poll", "n_intervals"),
    State("etl-last-completion", "data"),
)
def refresh_after_etl(_interval, previous_completion):
    completion = latest_etl_completion()
    if completion is not None and completion != previous_completion:
        return f"/?etl_updated={quote(completion, safe='')}"
    return no_update


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
