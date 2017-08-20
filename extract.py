#!/usr/bin/env python3

import json
import os.path
import re
import subprocess
import sys
import tempfile

from lxml import etree

import pandas as pd


OUT = '/media/data/500px-progressions-processed/'


def extract(archive, temp_dir):
    subprocess.check_call(
        'brotli -c -d {} | tar -xf - --strip=2'.format(archive),
        shell=True, cwd=temp_dir)


class ParsingTree(object):
    def __init__(self, target=str):
        self.target = target


class FirstItem(ParsingTree):
    def __call__(self, elements):
        if elements:
            return self.target(elements[0])
        else:
            return None


class Replace(ParsingTree):
    def __init__(self, target=str, search=None, replace=None):
        ParsingTree.__init__(self, target)
        self.search = search
        self.replace = replace

    def __call__(self, string):
        if string:
            return self.target(string.replace(self.search, self.replace))
        else:
            return None


PHOTO_XPATH_PARSE = [
    ("//meta[@property='five_hundred_pixels:category'][1]/@content",
     'meta-category',
     FirstItem()),
    ("//meta[@property='five_hundred_pixels:highest_rating'][1]/@content",
     'meta-highest_rating',
     FirstItem(float)),
    ("//meta[@property='five_hundred_pixels:location:latitude'][1]/@content",
     'meta-latitude',
     FirstItem(float)),
    ("//meta[@property='five_hundred_pixels:location:longitude'][1]/@content",
     'meta-longitude',
     FirstItem(float)),
    ("count(//meta[@property='five_hundred_pixels:tags'])",
     'meta-tags-count',
     int),
    ("//meta[@property='five_hundred_pixels:uploaded'][1]/@content",
     'meta-uploaded',
     FirstItem(pd.to_datetime)),
    ("//meta[@property='og:title'][1]/@content",
     'meta-title',
     FirstItem()),
    ("//meta[@property='og:description'][1]/@content",
     'meta-description',
     FirstItem()),
    ("//meta[@property='og:image:width'][1]/@content",
     'meta-image_width',
     FirstItem(int)),
    ("//meta[@property='og:image:height'][1]/@content",
     'meta-image_height',
     FirstItem(int)),
]


# TODO: check data type of "location"
PHOTO_JSON_PARSE = [
    ("camera", "json-camera", str),
    ("lens", "json-lens", str),
    ("focal_length", "json-focal_length", str),
    ("iso", "json-iso", int),
    ("shutter_speed", "json-shutter_speed", str),
    ("aperture", "json-aperture", str),
    ("times_viewed", "json-times_viewed", int),
    ("rating", "json-rating", float),
    ("status", "json-status", float),
    ("category", "json-category", int),
    ("location", "json-location", str),
    ("taken_at", "json-taken_at", pd.to_datetime),
    ("hi_res_uploaded", "json-hi_res_uploaded", int),
    ("for_sale", "json-for_sale", int),
    ("votes_count", "json-votes_count", int),
    ("favorites_count", "json-favorites_count", int),
    ("comments_count", "json-comments_count", int),
    ("nsfw", "json-nsfw", bool),
    ("sales_count", "json-sales_count", int),
    ("highest_rating", "json-highest_rating", float),
    ("highest_rating_date", "json-highest_rating_date", pd.to_datetime),
    ("license_type", "json-license_type", int),
    ("converted", "json-converted", bool),
    ("collections_count", "json-collections_count", int),
    ("positive_votes_count", "json-positive_votes_count", int),
    ("privacy", "json-privacy", bool),
    ("profile", "json-profile", bool),
    ("for_critique", "json-for_critique", bool),
    ("has_nsfw_tags", "json-has_nsfw_tags", bool),
    ("store_download", "json-store_download", bool),
    ("store_print", "json-store_print", bool),
    ("store_license", "json-store_license", bool),
    ("request_to_buy_enabled", "json-request_to_buy_enabled", bool),
    ("license_requests_enabled", "json-license_requests_enabled", bool),
    ("licensing_status", "json-licensing_status", int),
    ("licensing_type", "json-licensing_type", str),
    ("licensing_usage", "json-licensing_usage", str),
    ("editors_choice", "json-editors_choice", bool),
    ("editors_choice_date", "json-editors_choice_date", pd.to_datetime),
    ("feature", "json-feature", str),
    ("feature_date", "json-feature_date", pd.to_datetime),
    ("comments", "json-comments", len),
    ("watermark", "json-watermark", bool),
    ("licensing_requested", "json-licensing_requested", bool),
    ("licensing_suggested", "json-licensing_suggested", bool),
    ("is_free_photo", "json-is_free_photo", bool),
]

PHOTO_JSON_USER_PARSE = [
    ("username", "json-user-username", str),
    ("sex", "json-user-sex", int),
    ("city", "json-user-city", str),
    ("state", "json-user-state", str),
    ("country", "json-user-country", str),
    ("registration_date", "json-user-registration_date", pd.to_datetime),
    ("about", "json-user-about", str),
    ("usertype", "json-user-usertype", int),
    ("domain", "json-user-domain", str),
    ("fotomoto_on", "json-user-fotomoto_on", bool),
    ("show_nude", "json-user-show_nude", bool),
    ("allow_sale_requests", "json-user-allow_sale_requests", int),
    ("upgrade_status", "json-user-upgrade_status", int),
    ("store_on", "json-user-store_on", bool),
    ("affection", "json-user-affection", int),
    ("followers_count", "json-user-followers_count", int),
    ("contacts", "json-user-contacts", len),
    ("analytics_code", "json-user-analytics_code", str),
]


def parse_photo(folder):
    parser = etree.HTMLParser()
    with open(os.path.join(folder, "photo.html")) as html_file:
        root = etree.parse(html_file, parser)

    data = {}

    # Generic XPath parsing
    for xpath, target_key, parser in PHOTO_XPATH_PARSE:
        data[target_key] = parser(root.xpath(xpath))

    # Special JSON parsing of preload data
    json_data = json.loads(root.xpath(
        "//script[contains(text(), "
        "'window.PxPreloadedData')][1]/text()")[0].strip().replace(
            'window.PxPreloadedData = ', '')[:-1])['photo']
    for entry, target_key, parser in PHOTO_JSON_PARSE:
        try:
            if entry in json_data and json_data[entry] is not None:
                data[target_key] = parser(json_data[entry])
            else:
                data[target_key] = None
        except:
            print(entry)
            data[target_key] = None

    user_data = json_data['user'] or {}
    for entry, target_key, parser in PHOTO_JSON_USER_PARSE:
        try:
            if entry in user_data and user_data[entry] is not None:
                data[target_key] = parser(user_data[entry])
            else:
                data[target_key] = None
        except:
            print(entry)
            data[target_key] = None

    return data


USER_XPATH_PARSE = [
    ("//li[@class='views'][1]/span[1]/text()",
     'user-photo-views',
     FirstItem(Replace(int, ',', ''))),
    ("//li[@class='followers'][1]/span[1]/text()",
     'user-followers',
     FirstItem(Replace(int, ',', ''))),
    ("//li[@class='following'][1]/span[1]/text()",
     'user-following',
     FirstItem(Replace(int, ',', ''))),
    ("//li[@class='photos'][1]//span[@class='count'][1]/text()",
     'user-photos',
     FirstItem(Replace(int, ',', ''))),
    ("//li[@class='galleries'][1]//span[@class='count'][1]/text()",
     'user-galleries',
     FirstItem(Replace(int, ',', ''))),
    ("//li[@class='groups'][1]//span[@class='count'][1]/text()",
     'user-groups',
     FirstItem(Replace(int, ',', ''))),
    ("//li[@class='marketplace'][1]//span[@class='count'][1]/text()",
     'user-marketplace',
     FirstItem(Replace(int, ',', ''))),
]


def parse_user(folder):
    parser = etree.HTMLParser()
    with open(os.path.join(folder, "user.html")) as html_file:
        root = etree.parse(html_file, parser)

    data = {}

    for xpath, target_key, parser in USER_XPATH_PARSE:
        data[target_key] = parser(root.xpath(xpath))

    return data


def main(archive):

    with tempfile.TemporaryDirectory() as temp_dir:
        extract(archive, temp_dir)

        # iterate all timestamped folders
        all_data = {}
        for folder in [os.path.join(temp_dir, candidate)
                       for candidate in os.listdir(temp_dir)
                       if os.path.isdir(os.path.join(temp_dir, candidate))]:
            date = pd.to_datetime(os.path.basename(folder), unit='s')
            data = parse_photo(folder)
            data.update(parse_user(folder))
            all_data[date] = data

        all_data = pd.DataFrame.from_dict(all_data, orient='index')
        all_data.to_msgpack(os.path.join(
            OUT,
            os.path.basename(archive).split('-')[0] + '.msg'))


if __name__ == "__main__":
    main(os.path.abspath(sys.argv[1]))
