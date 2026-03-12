from workers.config import config

XENIUM_ARCHIVE_QUEUE = config.get('xenium_archive_queue', 'cmg-bioloop-xenium-archive.cmg-test.sca.iu.edu.q')
XENIUM_FETCH_QUEUE = config.get('xenium_fetch_queue', 'cmg-bioloop-xenium-fetch.cmg-test.sca.iu.edu.q')
