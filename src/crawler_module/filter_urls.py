import sys
sys.path.append("..")
from Utils import FrontingUtils

config = FrontingUtils.get_config()

#We will sort this later
FrontingUtils.filter_urls(config['FILE_PATHS']['cdn_domain_mapping_file_path'], config['FILE_PATHS']['domain_url_mapping_file_path'])