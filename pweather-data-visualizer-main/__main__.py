import logging
from typing import Dict, Tuple
import yaml
from downloader.downloader import download_files
from image_generator.image_generator import ImageGenerator
from properties_processor.processor import SettingsResults, process_settings

logging.basicConfig(level=logging.DEBUG,format='data-visualizer-%(levelname)s:%(message)s')

#--------------------------------------------------------
#process properties
props: SettingsResults
with open('properties.yaml', 'r') as f:
    props = process_settings(yaml.safe_load(f))

logging.info(props)

#--------------------------------------------------------
#download files
downloaded_files: Dict[Tuple[int,int],str] = download_files(props)

logging.debug(downloaded_files)

img_gen: ImageGenerator = ImageGenerator(props,downloaded_files)

img_gen.generate()