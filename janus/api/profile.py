import os
import yaml
import logging
from .utils import Constants
from janus import settings
from janus.settings import cfg
from janus.api.db import QueryUser
from janus.api.models import QoS_Controller, ContainerProfile, NetworkProfile, VolumeProfile


log = logging.getLogger(__name__)

class ProfileManager(QueryUser):
    CLSMAP = {
        Constants.HOST: ContainerProfile,
        Constants.NET: NetworkProfile,
        Constants.VOL: VolumeProfile,
        Constants.QOS: QoS_Controller
    }

    def __init__(self, db, profile_path = None):
        self._db = db
        self._profile_path = profile_path

    def get_profile_from_db(self, ptype=None, p=None, user=None, group=None):
        profile_tbl = self._db.get_table(ptype)
        query = self.query_builder(user, group, {"name": p})
        if query and p:
            ret = self._db.get(profile_tbl, query=query)
        elif query:
            ret = self._db.search(profile_tbl, query=query)
        else:
            ret = self._db.all(profile_tbl)
        return ret

    def get_profile(self, ptype, p, user=None, group=None, inline=False):
        ret = self.get_profile_from_db(ptype, p, user, group)
        return self.CLSMAP[ptype](**ret) if ret else None

    def get_profiles(self, ptype, user=None, group=None, inline=False):
        profiles = [ self.CLSMAP[ptype](**p) for p in self.get_profile_from_db(ptype, user=user, group=group) ]
        nprofs = len(profiles) if profiles else 0
        log.info(f"total profiles: {nprofs}")
        return profiles

    def read_profiles(self, path=None, reset=False, refresh=False):
        image_tbl = self._db.get_table('images')
        for img in settings.SUPPORTED_IMAGES:
            ni = {"name": img}
            self._db.upsert(image_tbl, ni, 'name', img)
        if not refresh:
            return
        host_tbl = self._db.get_table(Constants.HOST)
        vol_tbl = self._db.get_table(Constants.VOL)
        net_tbl = self._db.get_table(Constants.NET)
        qos_tbl = self._db.get_table(Constants.QOS)
        if reset:
            host_tbl.truncate()
            vol_tbl.truncate()
            net_tbl.truncate()
            qos_tbl.truncate()
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
                                if (k == "networks"):
                                    for key, value in v.items():
                                        try:
                                            prof = {"name": key, "settings": value}
                                            NetworkProfile(**prof)
                                            cfg._networks[key] = value
                                            self._db.upsert(net_tbl, prof, 'name', key)
                                        except Exception as e:
                                            log.error("Error reading networks: {}".format(e))

                                if (k == "volumes"):
                                    for key, value in v.items():
                                        try:
                                            prof = {"name": key, "settings": value}
                                            VolumeProfile(**prof)
                                            cfg._volumes[key] = value
                                            self._db.upsert(vol_tbl, prof, 'name', key)
                                        except Exception as e:
                                            log.error("Error reading volumes: {}".format(e))

                                if (k == "qos"):
                                    for key, value in v.items():
                                        try:
                                            prof = {"name": key, "settings": value}
                                            QoS_Controller(**prof)
                                            cfg._qos[key] = value
                                            self._db.upsert(qos_tbl, prof, 'name', key)
                                        except Exception as e:
                                            log.error("Error reading qos: {}".format(e))

                                if (k == "profiles"):
                                    for key, value in v.items():
                                        try:
                                            temp = cfg._base_profile.copy()
                                            temp.update(value)
                                            prof = {"name": key, "settings": temp}
                                            ContainerProfile(**prof)
                                            cfg._profiles[key] = temp
                                            self._db.upsert(host_tbl, prof, 'name', key)
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
        log.info("features: {}".format(cfg._features.keys()))
        log.info("profiles: {}".format(cfg._profiles.keys()))
        log.info("networks: {}".format(cfg._networks.keys()))

