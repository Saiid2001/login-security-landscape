import sys
import time
import traceback
from api import print, unlock_session
import db
from typing import List


def main() -> int:
    """Expire sessions that are too old (new validation tasks are scheduled)."""
    # Main loop
    try:
        with db.db.atomic():
            sessions: List[db.Session] = (
                db.Session.select()
                .join(db.SessionStatus)
                .where(
                    db.Session.experiment != None,
                )
            )
            
            for session in sessions:
                unlock_session(session)
    except Exception as error:
        traceback.print_exc()
        print(error)

    return 0


if __name__ == "__main__":
    sys.exit(main())
