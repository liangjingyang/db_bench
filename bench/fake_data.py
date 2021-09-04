import argparse
import copy
import math
import os
import uuid
from multiprocessing import Pool
from random import randrange, random

from faker import Faker

from bench.models import RCommit
from bench.session import get_db_session, create_session_maker


def run_fake(args):
    pid = os.getpid()
    print(f'Process {pid} started, to fake {args.to_fake}.')
    maker = create_session_maker(args.db_url)
    faker = Faker()
    with get_db_session(maker) as session:
        batch = 0
        total = 0
        analysis_id = str(uuid.uuid4())
        for i in range(args.to_fake):
            unix_time = faker.unix_time()
            r_commit = RCommit()
            r_commit.analysis_id = analysis_id
            r_commit.hexsha = faker.text(40)
            r_commit.author_email = faker.email()
            r_commit.author_name = faker.name()
            r_commit.author_time = unix_time
            r_commit.author_offset = 0
            r_commit.committer_email = faker.email()
            r_commit.committer_name = faker.name()
            r_commit.committer_time = unix_time
            r_commit.committer_offset = 0
            r_commit.message = faker.text(50)
            r_commit.parents = faker.text(50)
            r_commit.files_changed = randrange(1000)
            r_commit.insertions = randrange(1000)
            r_commit.deletions = randrange(1000)
            r_commit.supported_insertions = randrange(1000)
            r_commit.supported_deletions = randrange(1000)
            r_commit.complexity = randrange(1000)
            r_commit.cyclomatic_complexity = randrange(20)
            r_commit.big_cc_func_count = randrange(20)
            r_commit.cherry_pick_from = faker.text(40)
            r_commit.large_insertion = True if randrange(2) == 1 else False
            r_commit.large_deletion = True if randrange(2) == 1 else False
            r_commit.revert = True if randrange(2) == 1 else False
            r_commit.dev_eq = randrange(1000)
            r_commit.dev_rank = random()
            session.add(r_commit)
            batch += 1
            if batch == 1000:
                total += batch
                if total % 10000 == 0:
                    analysis_id = str(uuid.uuid4())
                print(f'Process {pid} committed {total} records.')
                batch = 0
                session.commit()
        session.commit()


def start_fake(args):
    with Pool(args.concurrency) as p:
        left = args.number - args.count
        to_fake = math.ceil(left / args.concurrency)
        maps = []
        faked = 0
        for i in range(args.concurrency):
            copied_args = copy.deepcopy(args)
            copied_args.to_fake = to_fake
            if i == copied_args.concurrency - 1:
                copied_args.to_fake = left - faked
            maps.append(copied_args)
            faked += to_fake
        p.map(run_fake, maps)


def run(args):
    db_url = args.db_url
    number = args.number
    maker = create_session_maker(db_url)
    with get_db_session(maker) as session:
        count = session.query(RCommit).count()
        if count >= number:
            print(f'{count} existed, no record faked.')
            return
        else:
            print(f'{count} existed, {number - count} to be faked.')
            args.count = count
            start_fake(args)


parser = argparse.ArgumentParser(prog='DB Bench', description='fake data')
parser.add_argument('--url', dest='db_url', type=str,
                    help='the db url, e.g. postgresql://postgres:postgres@postgres:5432/db_bench')
parser.add_argument('--n',
                    dest='number',
                    type=int,
                    help='fake record until reaching this number')
parser.add_argument('--c',
                    dest='concurrency',
                    type=int,
                    help='start how many process to fake data')

if __name__ == '__main__':
    run(parser.parse_args())
