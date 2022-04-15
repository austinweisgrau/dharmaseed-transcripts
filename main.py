import logging
import json
import re
import sqlite3
import tempfile
import time

import boto3
import pandas as pd
import requests
from bs4 import BeautifulSoup

RSS_LINK = "https://dharmaseed.org/feeds/teacher/148/?max-entries=all"
DB_PATH = 'db.sql'

logging.basicConfig(level=logging.INFO)


def fetch_rss() -> BeautifulSoup:
    rss_resp = requests.get(RSS_LINK)
    logging.info('Fetched RSS.')
    return BeautifulSoup(rss_resp.content, features="xml")


def parse_rss(rss_bs: BeautifulSoup = None) -> pd.DataFrame:
    if not rss_bs:
        rss_bs = fetch_rss()
    talks = rss_bs.findAll("item")
    result = []
    for talk in talks:
        talk_item = {}
        for attr in talk.contents:
            talk_item.update({attr.name: attr.string})
        talk_item['mp3_link'] = talk.enclosure.get('url')
        result.append(talk_item)
    df = pd.DataFrame(result)
    logging.info('Retrieved talks. [n={}]'.format(len(df)))
    return df[["title", "link", "mp3_link", "description", "pubDate", "guid"]]


def save_talks(talks_data: pd.DataFrame = None) -> None:
    if not talks_data:
        talks_data = parse_rss()
    conn = sqlite3.connect(DB_PATH)
    talks_data.set_index('guid')
    talks_data.to_sql('talks', conn, if_exists="replace")


def save_transcription_from_talk(talk_guid: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    talk_title, talk_link = conn.execute(
        'select title, mp3_link from talks where guid = ?',
        (talk_guid, )).fetchone()
    logging.info('Fetching talk. [title={}]'.format(talk_title))
    talk_data = {'title': talk_title, 'link': talk_link, 'guid': talk_guid}
    upload_talk_audio(talk_data)
    await_transcription_job(transcribe_talk(talk_data))
    download_transcription(talk_guid)


def upload_talk_audio(talk_data: dict) -> None:
    """Uses talk GUID as s3 key."""
    conn = sqlite3.connect(DB_PATH)
    talk_link, talk_title = conn.execute(
        'select mp3_link, title from talks where guid = ?',
        (talk_data['guid'], )).fetchone()

    boto3.client('s3').upload_fileobj(
        requests.get(talk_link, stream=True).raw, 'rs-dharma-audio',
        talk_data['guid'])
    logging.info('Uploaded talk audio to s3. [title={}]'.format(
        talk_data['title']))


def transcribe_talk(talk_data: dict) -> None:
    ts = boto3.client('transcribe')
    job_name = re.sub(r'\W+', '', talk_data['title'])
    job_created = False
    i = 1
    while not job_created:
        try:
            ts.start_transcription_job(
                TranscriptionJobName=job_name,
                LanguageCode='en-US',
                Media={
                    'MediaFileUri': 's3://rs-dharma-audio/' + talk_data['guid']
                },
                OutputBucketName='rs-dharma-transcriptions',
                OutputKey=talk_data['guid'] + '_standard',
            )
            job_created = True
        except ts.exceptions.ConflictException:
            job_name = '_'.join(job_name.split('_')[:-1]) + '_' + str(i)
            i += 1
    logging.info('Began transcription job. [job_name={}]'.format(job_name))
    return job_name


def await_transcription_job(transcription_job_name: str):
    ts = boto3.client('transcribe')
    status = ''
    while status not in ('FAILED', 'COMPLETED'):
        job = ts.get_transcription_job(
            TranscriptionJobName=transcription_job_name)
        status = job['TranscriptionJob']['TranscriptionJobStatus']
        logging.info('Current status: {}'.format(status))
        time.sleep(30)
    if status == 'FAILED':
        logging.error('Transcription job failed. [job={}, reason={}]'.format(
            transcription_job_name, job['TranscriptionJob']['FailureReason']))


def download_transcription(talk_guid: str,
                           transcription_type: str = 'standard') -> None:
    s3 = boto3.client('s3')
    savepath = tempfile.mkstemp()[1]
    transcription_file = s3.download_file('rs-dharma-transcriptions',
                                          talk_guid + '_' + transcription_type,
                                          savepath)
    logging.info('Downloaded transcription')

    with open(savepath, 'r') as f:
        transcription = json.loads(
            f.read())['results']['transcripts'][0]['transcript']

    df = pd.DataFrame([{
        'guid': talk_guid,
        'transcription': transcription
    }]).set_index('guid')

    conn = sqlite3.connect(DB_PATH)
    df.to_sql('transcriptions', conn, if_exists='append')
    logging.info('Saved transcription in DB')


def download_one_new_talk() -> None:
    conn = sqlite3.connect(DB_PATH)
    guid = conn.execute('''
      select ta.guid from talks ta
      left join transcriptions tr
        on ta.guid = tr.guid
      where tr.transcription is null
      limit 1
      ''').fetchone()[0]
    save_transcription_from_talk(guid)
    download_transcription(guid)


if __name__ == '__main__':
    download_one_new_talk()
