import os
import yaml
import logging
from janus import settings
from janus.settings import cfg
from janus.api.db import QueryUser
from janus.api.validator import QoS_Controller, Profile


log = logging.getLogger(__name__)

class ProfileManager(QueryUser):
    def __init__(self, db, profile_path = None):
        self._db = db
        self._profile_path = profile_path

    def get_profile_from_db(self, p=None, user=None, group=None):
        profile_tbl = self._db.get_table('profiles')
        query = self.query_builder(user, group, {"name": p})
        if query and p:
            ret = self._db.get(profile_tbl, query=query)
        elif query:
            ret = self._db.search(profile_tbl, query=query)
        else:
            ret = self._db.all(profile_tbl)
        return ret

    def get_profile(self, p, user=None, group=None, inline=False):
        return self.get_profile_from_db(p, user, group)

    def get_profiles(self, user=None, group=None, inline=False):
        ret = dict()
        profiles = self.get_profile_from_db(user=user, group=group)
        nprofs = len(profiles) if profiles else 0
        log.info(f"total profiles: {nprofs}")
        return profiles

    def read_profiles(self, path=None, reset=False):
        image_tbl = self._db.get_table('images')
        for img in settings.SUPPORTED_IMAGES:
            ni = {"name": img}
            self._db.upsert(image_tbl, ni, 'name', img)
        profile_tbl = self._db.get_table('profiles')
        if reset:
            profile_tbl.truncate()
        if not path:
            path = self._profile_path
        if not path:
            raise Exception("Profile path is not set")
        for f in os.listdir(path):
            entry = os.path.join(path, f)
            if os.path.isfile(entry) and (f.endswith(".yml") or f.endswith(".yaml")):
                with open(entry, "r") as yfile:
                    try:
                        data = yaml.safe_load(yfile)
                        for k, v in data.items():
                            if isinstance(v, dict):
                                if (k == "volumes"):
                                    cfg._volumes.update(v)

                                if (k == "qos"):
                                    for key, value in v.items():
                                        try:
                                            QoS_Controller(**value)
                                            cfg._qos[key] = value
                                        except Exception as e:
                                            log.error("Error reading qos: {}".format(e))

                                if (k == "profiles"):
                                    for key, value in v.items():
                                        try:
                                            temp = cfg._base_profile.copy()
                                            temp.update(value)
                                            Profile(**temp)
                                            cfg._profiles[key] = temp
                                            self._db.upsert(profile_tbl, {"name": key, "settings": temp}, 'name', key)
                                        except Exception as e:
                                            log.error("Error reading profiles: {}".format(e))

                                if (k == "features"):
                                    cfg._features.update(v)

                                if (k == "post_starts"):
                                    cfg._post_starts.update(v)

                    except Exception as e:
                        raise Exception(f"Could not load configuration file: {entry}: {e}")
                    yfile.close()

        log.info("qos: {}".format(cfg._qos.keys()))
        log.info("volumes: {}".format(cfg._volumes.keys()))
        log.info("volumes: {}".format(cfg._volumes.keys()))
        log.info("features: {}".format(cfg._features.keys()))
        log.info("profiles: {}".format(cfg._profiles.keys()))


