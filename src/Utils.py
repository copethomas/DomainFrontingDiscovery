import os
import json
import tldextract
from configparser import ConfigParser, ExtendedInterpolation

import pandas as pd
from numpy.f2py.auxfuncs import throw_error


class FrontingUtils:

    @staticmethod
    def get_SLD(domain):
            try:
                extract = tldextract.TLDExtract()
                ext = extract(domain)
                return ext.registered_domain
            except Exception as e:
                return domain
    
    @staticmethod
    def get_config():
        # Create a ConfigParser object with ExtendedInterpolation
        config = ConfigParser(interpolation=ExtendedInterpolation())
        config.read( os.path.dirname(os.path.abspath(__file__))+'/config.ini')
        return config

    @staticmethod
    def get_full_domain(url):
        try:
            extract = tldextract.TLDExtract()
            ext = extract(url)
            # Access the named attributes of ExtractResult
            parts = [ext.subdomain, ext.domain, ext.suffix]
            return '.'.join(part for part in parts if part)
        except Exception as e:
            print(e)
            return None

    @staticmethod
    def filter_urls(cdn_domain_file, domain_urls_file):
        resources = []

        df_cdn_domains = pd.read_csv(cdn_domain_file, header=0)
        crawler_results_path = FrontingUtils.get_config()['FILE_PATHS']['crawling_results_path']

        all_cdns = df_cdn_domains['cdn'].unique().tolist()

        for dir in os.listdir(crawler_results_path):
            headers = {}

            matched_cdn = ""
            for c in all_cdns:
                #We do this because we can't just split by a "_" because I was silly and included a "_" in some of the CDN names
                if dir.startswith(c):
                    matched_cdn = c

            if matched_cdn == "":
                raise "did not match CDN for dir"

            visited_domain = dir[len(matched_cdn)+1:]
            print("Processing = " + visited_domain)
            target_row = df_cdn_domains[df_cdn_domains['domain_sld'] == visited_domain]
            cdn_name = target_row['cdn'].iloc[0]
            related_domains = df_cdn_domains[df_cdn_domains['cdn'] == cdn_name]['domain_sld'].tolist()
            file = os.path.join(crawler_results_path, dir,visited_domain+'_headers.json')

            try:
                with open(file,'r') as f:
                    headers = json.load(f)
            except FileNotFoundError as e:
                # Something went wrong when scraping this, skip it.
                continue


            res_count = 0
            for rec in headers['table']:
                #print("debug -> " + rec['response_url'])
                url_dom = FrontingUtils.get_full_domain(rec['response_url'])

                ### Filter URLs to only retain those that share the same domain that's of interest
                if url_dom in related_domains :
                    try:
                        res_det = {'cdn': dir.split('_')[0],
                                        'visited_domain': visited_domain,
                                        'original_domain': url_dom,
                                        'resource_url': rec['response_url'],
                                        'content_type': rec['header']['content-type'],
                                        'server_ip': rec['server_info']['ip']
                                }
                    except KeyError as e:
                        # don't have all the details required from the scrape, skip it
                        continue

                    resources.append(res_det)
                    res_count += 1


        print("Saving to file = " + domain_urls_file + " ...")
        with open(domain_urls_file,'w') as f:
            json.dump(resources, f, indent=2)