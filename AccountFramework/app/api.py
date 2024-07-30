from collections import defaultdict
import os
import sys
import traceback
from typing import Optional
import db
import json
import zmq
from playhouse.shortcuts import model_to_dict
import datetime
import pathlib
from run_auto import Tee

import functools

print = functools.partial(print, flush=True)
_print = print


def print(*args, **kw):
    _print("[%s]" % (datetime.datetime.now()), *args, **kw)


SESSION_FILE_PATH = "./auth/"
LOG_FILE = "logs/00_0_api.log"

# Messages:
# --- Client -> Server ---
# {"type": "get_session", "experiment": <experiment name>}
# get a session from the server
#
# {"type": "get_specific_session", "experiment": <experiment name>, "site": <site>}
# get a session for a specific site from the server (e.g., "site": "example.org")
#
# {"type": "unlock_session", "session_id": <id>, "experiment": <experiment name>}
# unlocks a previously claimed session
#
# --- Server -> Client ---
# {"success": "false", "error": <error_message>}
# error response to client (something the client wanted to do didn't work)
#
# {"success": "true"}
# answer to successful "unlock_session" request
#
# {"success": "true", "session": <session>, "session_data": <json>, "loginform": <loginform>}
# answer to sucessful "get_session" or "get_specific_session" request.
# <session> is a database dump of the session entry
# <json> is a json object containing cookies and local storage associated with the session
# <loginform> is a database dump of the loginform entry


global socket


def send_success(data):
    global socket
    msg = {"success": True}
    msg.update(data)
    print("success")
    socket.send_string(json.dumps(msg, default=str))


def send_error(error):
    global socket
    print(error)
    socket.send_string(json.dumps({"success": False, "error": error}))


def handle_unlock_session(experiment: str, session_id: str):
    """Handle an unlock request. Reschedule validation task."""
    print(f"Unlock session for experiment: {experiment}, session: {session_id}")
    # Handle unlock session
    if type(experiment) != str:
        send_error("Experiment is required!")
    else:
        session = db.Session.get_or_none(
            db.Session.id == session_id, db.Session.experiment == experiment
        )
        if session:
            unlock_session(session)
            send_success(dict())
        else:
            send_error("Session does not exist or does not belong to the experiment!")


def unlock_session(session: db.Session):
    """Unlocks a session and schedule a new validation task."""
    session.locked = False
    session.verified = False
    session.verify_type = "no"
    session.verified_browsers = ""
    session.session_status = db.SessionStatus.get(db.SessionStatus.name == "expired")
    session.unlock_time = datetime.datetime.now()
    session.experiment = None
    session.save()
    db.ValidateTask.create(session=session, task_type="auto")


def unlock_old_sessions():
    """Unlock sessions that should already be unlocked (automatic unlock time is due)."""
    current_time = datetime.datetime.now()
    old_sessions: list[db.Session] = db.Session.select().where(
        db.Session.locked == True, db.Session.unlock_time <= current_time
    )
    for session in old_sessions:
        t_delta = current_time - session.unlock_time
        print(
            f"Experiment={session.experiment} did not unlock session {session} for website {session.account.website.site} in time. Should already be unlocked for {t_delta}"
        )
        unlock_session(session)


def expire_old_sessions(sessions: list[db.Session]) -> list[db.Session]:
    """If a session is too old (update time is older than N), mark as expired and schedule new validation tasks (only for sessions that are not locked)."""
    current_time = datetime.datetime.now()
    usable_sessions = []
    for session in sessions:
        auto_limit = int(os.getenv("AUTO_VERIFY_TIMOUT", "12"))
        manual_limit = int(os.getenv("MANUAL_VERIFY_TIMOUT", "12"))
        if session.verify_type == "auto":
            limit = datetime.timedelta(hours=auto_limit)
        elif session.verify_type == "manual":
            limit = datetime.timedelta(hours=manual_limit)
        else:
            raise Exception(
                f"verify_type={session.verify_type} is invalid in expiration of old sessions"
            )

        # If update time is too old, schedule new validation tasks
        if (current_time - session.update_time) > limit:
            print(
                f"Session: {session} for website {session.account.website.site} was not used before expiration. Schedule new valdidation task!"
            )
            unlock_session(session)
        else:
            usable_sessions.append(session)

    return usable_sessions


def handle_get_session(experiment: str, site=None):
    """Handle a session request. An experiment name is required. Optional a site can be specified to request a session for that specific site."""
    global SESSION_FILE_PATH
    print(f"Get session for experiment: {experiment}")

    # Get all currently available sessions (active + verified + unlocked)
    sessions: list[db.Session] = (
        db.Session.select()
        .join(db.SessionStatus)
        .where(
            db.SessionStatus.active == True,
            db.Session.locked == False,
            db.Session.verified == True,
        )
    )

    # Expire old sessions (schedule new validation tasks)
    sessions: list[db.Session] = expire_old_sessions(sessions)
    # Automatically unlock sessions that were not unlocked in time
    # Schedule new valdidate tasks for these sessions!
    unlock_old_sessions()

    # If a specific site is requested
    # Return a session for the requested site (regardless of whether it was used already by the experiment)
    if site:
        print(f"{site} requested by {experiment}")
        # Iterate over all available sessions; if one fits use it
        new_sessions = []
        for session in sessions:
            if session.account.website.site == site:
                new_sessions = [session]
                break
        sessions = new_sessions

    # Return any session that was not already given to the site
    else:
        # Get websites already given to this experiment (In the future, experiments could have several accounts for the same website and we need to adapt the logic)
        websites_used = [
            w.website
            for w in db.ExperimentWebsite.select().where(
                db.ExperimentWebsite.experiment == experiment
            )
        ]
        # Subset of sessions that are not used already
        sessions = [
            session
            for session in sessions
            if session.account.website not in websites_used
        ]

    # Use the first availabe session, lock it and return it to the experiment
    if len(sessions):
        session = sessions[0]
        # Lock session and assign experiment to it!
        session.locked = True
        session.unlock_time = datetime.datetime.now() + datetime.timedelta(
            hours=int(os.getenv("TIMEOUT_EXP_SESSION", "24"))
        )
        session.experiment = experiment
        session.save()

        # Remember the website and do not hand it out again to the same experiment (if no specific site is requested)
        if session.account and site is None:
            db.ExperimentWebsite.create(
                website=session.account.website, experiment=experiment, session=session
            )
        elif site is None:
            print(f"[WARN] account for session {session.id} is None")

        loginform: Optional[aa_LoginForm] = aa_LoginForm.get_or_none(
            site=session.account.website.site, success=True
        )
        loginform = loginform or aa_LoginForm.get_or_none(
            site=session.account.website.site
        )

        # Send the session to the client
        if loginform is None:
            send_success(
                {
                    "session": model_to_dict(session),
                    "session_data": json.loads(
                        open(f"{SESSION_FILE_PATH}{session.name}.json").read()
                    ),
                }
            )
        else:
            send_success(
                {
                    "session": model_to_dict(session),
                    "session_data": json.loads(
                        open(f"{SESSION_FILE_PATH}{session.name}.json").read()
                    ),
                    "loginform": model_to_dict(loginform),
                }
            )

    # If there is no sessions left, we need to send an error
    else:
        send_error("no sessions available")
        return


def handle_get_sessions(experiment: str, site=None, k=2):
    """Handle multiple session requests. An experiment name is required. Optional a site can be specified to request a session for that specific site."""

    global SESSION_FILE_PATH
    print(f"Get sessions for experiment: {experiment}")

    # Get all currently available sessions (active + verified + unlocked) such that there are at least k sessions from different accounts per website

    available_websites = (
        db.Account.select(
            db.fn.Count(db.Account.id.distinct()).alias("account_count"),
            db.Account.website,
        )
        .join(db.Session, on=(db.Account.session == db.Session.id))
        .join(db.SessionStatus, on=(db.Session.session_status == db.SessionStatus.id))
        .where(
            db.SessionStatus.active == True,
            db.Session.locked == False,
            db.Session.verified == True,
        )
        .group_by(db.Account.website)
        .having(db.fn.Count(db.Account.id.distinct()) >= k)
    )

    #     # query result
    #     # | session_id | account_id | website_id |
    #     # |------------|------------|------------|
    #     # | 1          | 1          | 1          |
    #     # | 2          | 2          | 1          |
    #     # | 3          | 3          | 2          |
    #     # | 4          | 4          | 2          |

    query = (
        db.Account.select(
            db.Account.id,
            db.Account.session,
            db.Account.website,
        )
        .where(
            db.SessionStatus.active == True,
            db.Session.locked == False,
            db.Session.verified == True,
        )
        .join(
            available_websites,
            on=(db.Account.website == available_websites.c.website_id),
        )
        .join(db.Session, on=(db.Account.session == db.Session.id))
        .join(db.SessionStatus, on=(db.Session.session_status == db.SessionStatus.id))
        .order_by(available_websites.c.website_id)
    )

    sessions = []
    sessions_per_site = defaultdict(list)

    for row in query:
        sessions_per_site[row.website.site].append(row.session)
        sessions.append(row.session)

    # Expire old sessions (schedule new validation tasks)
    sessions: list[db.Session] = expire_old_sessions(sessions)

    # Automatically unlock sessions that were not unlocked in time
    # Schedule new valdidate tasks for these sessions!
    unlock_old_sessions()

    # check if some sites need to be removed again
    for site, site_sessions in list(sessions_per_site.items()):
        _remaining_sessions = list(set(site_sessions) & set(sessions))

        if len(_remaining_sessions) < k:
            del sessions_per_site[site]

        # keep only k sessions per site
        sessions_per_site[site] = _remaining_sessions[:k]
        
    # If a specific site is requested
    # Return a session for the requested site (regardless of whether it was used already by the experiment)
    if site:
        print(f"{site} requested by {experiment}")
        # Iterate over all available sessions; if one fits use it

        sessions_per_site = (
            {site: sessions_per_site[site]} if site in sessions_per_site else {}
        )

    # Return any session that was not already given to the site
    else:
        # Get websites already given to this experiment (In the future, experiments could have several accounts for the same website and we need to adapt the logic)
        sites_used = [
            w.website.site
            for w in db.ExperimentWebsite.select().where(
                db.ExperimentWebsite.experiment == experiment
            )
        ]

        for site, site_sessions in list(sessions_per_site.items()):
            if site in sites_used:
                del sessions_per_site[site]

    # Use the first availabe session, lock it and return it to the experiment
    if len(sessions_per_site):
        # session = sessions[0]
        site, site_sessions = list(sessions_per_site.items())[0]

        session_responses = {"site": site, "sessions": []}

        for session in site_sessions:
            # Lock session and assign experiment to it!
            session.locked = True
            session.unlock_time = datetime.datetime.now() + datetime.timedelta(
                hours=int(os.getenv("TIMEOUT_EXP_SESSION", "24"))
            )
            session.experiment = experiment
            session.save()

            # Remember the website and do not hand it out again to the same experiment (if no specific site is requested)
            if session.account and site is None:
                db.ExperimentWebsite.create(
                    website=session.account.website,
                    experiment=experiment,
                    session=session,
                )
            elif site is None:
                print(f"[WARN] account for session {session.id} is None")

            loginform: Optional[aa_LoginForm] = aa_LoginForm.get_or_none(
                site=session.account.website.site, success=True
            )
            loginform = loginform or aa_LoginForm.get_or_none(
                site=session.account.website.site
            )

            # Send the session to the client
            if loginform is None:
                session_responses["sessions"].append(
                    {
                        "session": model_to_dict(session),
                        "session_data": json.loads(
                            open(f"{SESSION_FILE_PATH}{session.name}.json").read()
                        ),
                    }
                )
            else:
                session_responses["sessions"].append(
                    {
                        "session": model_to_dict(session),
                        "session_data": json.loads(
                            open(f"{SESSION_FILE_PATH}{session.name}.json").read()
                        ),
                        "loginform": model_to_dict(loginform),
                    }
                )

        send_success(session_responses)

    # If there is no sessions left, we need to send an error
    else:
        send_error("no sessions available")
        return


if __name__ == "__main__":
    """Main loop. Wait for API requests and serve sessions if requested and available."""
    sys.path = [
        str((pathlib.Path(__file__).parent / "account_automation").resolve())
    ] + sys.path
    from account_automation.modules.findloginforms import aa_LoginForm

    with Tee(LOG_FILE, "API"):
        # Start zmq server
        print("Start API!")
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind(f"tcp://0.0.0.0:{os.getenv('ZMQ_PORT')}")
        print("Started API")
        while True:
            # Receive a message. The next sending on the socket will automatically send to the correct client
            raw_data = socket.recv_string()

            try:
                # We should receive a json string
                request: dict = json.loads(raw_data)

                # Handle request (depending on "type" field)
                if request["type"] == "get_session":
                    handle_get_session(request["experiment"])
                elif request["type"] == "get_specific_session":
                    handle_get_session(request["experiment"], request["site"])
                elif request["type"] == "get_sessions":
                    handle_get_sessions(request["experiment"], k=request.get("k", 2))
                elif request["type"] == "get_specific_sessions":
                    handle_get_sessions(request["experiment"], request["site"], k=request.get("k", 2))
                elif request["type"] == "unlock_session":
                    handle_unlock_session(request["experiment"], request["session_id"])
                else:
                    send_error(f"illegal request type {request['type']}")

            except Exception as e:
                # Something went wrong. Just send a generic error message.
                print(traceback.format_exc())
                send_error(f"invalid request {e}")
