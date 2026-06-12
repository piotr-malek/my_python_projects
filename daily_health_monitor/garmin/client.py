import time

from util.retry import with_retry


class GarminClient:
    _instance = None

    def __init__(self, settings):
        self._settings = settings
        self._api = None

    @classmethod
    def get(cls, settings):
        if cls._instance is None:
            cls._instance = cls(settings)
        return cls._instance

    @property
    def api(self):
        if self._api is None:
            from garminconnect import Garmin

            self._api = Garmin(
                self._settings.GARMIN_EMAIL,
                self._settings.GARMIN_PASSWORD,
                prompt_mfa=lambda: input("Garmin MFA code: ").strip(),
            )
            self._api.login(str(self._settings.GARMINTOKENS))
        return self._api

    def call(self, fn, *args, retries=3, **kwargs):
        return with_retry(fn, *args, retries=retries, **kwargs)
