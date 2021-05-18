import sys
import re
from os.path import isfile, isdir, join, exists
from os import listdir, makedirs, rename
from multiprocessing import Pool
from subprocess import call

TREC_PREFIX = 'trec-'


def process_sub_trec(sub_trecs_folder, qrel, sub_run, filename):
    output_path = join(sub_trecs_folder, filename)
    tmp_output_path = output_path + ".tmp"
    if not isfile(output_path) and not isfile(tmp_output_path):
        with open(tmp_output_path, 'w+') as sub_trec:
            call(['rec_eval', '-q', '-c', '-m', 'recsys', '-l', '1', qrel, sub_run],
                 stdout=sub_trec)
        rename(tmp_output_path, output_path)


def run_trec(runs_folder, qrels_folder, trecs_folder):
    # Get all run folders (each run is divided in subruns stored in run folders)
    folders = [f for f in listdir(runs_folder) if isdir(join(runs_folder, f))]
    sub_runs_args = []

    for folder in folders:
        regexp = re.match(r""".*fold(?P<fold>[0-9]+)""", folder)

        try:
            fold = int(regexp.group("fold"))
        except AttributeError as exc:
            raise AttributeError(str(exc) + " in " + folder)
        # Get all subruns for this run folder
        run_folder = join(runs_folder, folder)
        qrel = join(qrels_folder, "u{0}.qrel".format(fold))
        sub_runs_filenames = [f for f in listdir(run_folder) if isfile(join(run_folder, f))]
        # We'll store the trecs for each subrun in a subtrecs folder for now
        sub_trecs_folder = join(trecs_folder, TREC_PREFIX + folder)
        create_if_necessary(sub_trecs_folder)
        # Prepare args for each eval task
        for filename in sub_runs_filenames:
            sub_run = join(run_folder, filename)
            sub_runs_args.append((sub_trecs_folder, qrel, sub_run, TREC_PREFIX + filename))

    with Pool(processes=8) as pool:
        pool.starmap(process_sub_trec, sub_runs_args)
        pool.close()
        pool.join()


def create_if_necessary(folder):
    if not isdir(folder):
        if exists(folder):
            print("{0} already exists and it is not a folder".format(folder))
        else:
            makedirs(folder)


def main():
    if len(sys.argv) < 4:
        print("{0} <runs_folder> <qrels_folder> <trecs_folder>"
              .format(sys.argv[0]))
        sys.exit(0)

    runs_folder = sys.argv[1]
    qrels_folder = sys.argv[2]
    trecs_folder = sys.argv[3]

    create_if_necessary(trecs_folder)
    run_trec(runs_folder, qrels_folder, trecs_folder)


if __name__ == '__main__':
    main()
