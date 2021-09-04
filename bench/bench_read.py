import argparse
import copy
import math
import time

from multiprocessing import Pool
from random import randrange
from typing import List

from bench.models import RCommit
from bench.session import create_session_maker, get_db_session


def target(args):
    maker = create_session_maker(args.db_url)
    analysis_ids = args.analysis_ids
    if not analysis_ids:
        print('analysis_ids is empty.')
        return
    index = 0

    def get_analysis_id():
        nonlocal index
        if index == len(analysis_ids):
            index = 0
        return analysis_ids[index]

    t0 = time.perf_counter()
    with get_db_session(maker) as session:
        for i in range(args.to_query):
            r_commits: List[RCommit] = session.query(RCommit).filter(
                RCommit.analysis_id == get_analysis_id()
            ).offset(randrange(1, 9999)).limit(2000).all()
    t1 = time.perf_counter()
    cost = t1 - t0
    print('total:', t1 - t0)
    return len(r_commits), cost, args.times


def run(args):
    with Pool(args.concurrency) as p:
        to_query = math.ceil(args.times / args.concurrency)
        maker = create_session_maker(args.db_url)
        with get_db_session(maker) as session:
            analysis_ids = RCommit.get_rand_analysis_id(session)
            analysis_ids = [item for sublist in analysis_ids for item in sublist]
            args.analysis_ids = analysis_ids
        print(args.analysis_ids)
        maps = []
        queried = 0
        for i in range(args.concurrency):
            copied_args = copy.deepcopy(args)
            copied_args.to_query = to_query
            if i == copied_args.concurrency - 1:
                copied_args.to_query = args.times - queried
            maps.append(copied_args)
            queried += to_query
        result = p.map(target, maps)
        for r in result:
            print(r)


parser = argparse.ArgumentParser(prog='DB Bench', description='bench read')
parser.add_argument('--url', dest='db_url', type=str,
                    help='the db url, e.g. postgresql://postgres:postgres@postgres:5432/db_bench')
parser.add_argument('--t',
                    dest='times',
                    type=int,
                    help='query how many times')
parser.add_argument('--c',
                    dest='concurrency',
                    type=int,
                    help='start how many process to fake data')

if __name__ == '__main__':
    run(parser.parse_args())
