import sys
import re
from os.path import isfile, isdir, join, exists
from os import listdir, makedirs, rename
from multiprocessing import Pool

TREC_MEASURES = {'P_5', 'P_10', 'P_100', 'recip_rank', 'err', 'set_F', 'ndcg_cut_5',
                 'ndcg_cut_10', 'ndcg_cut_100', 'map', 'bpref', 'infAP2', 'rmse', 'mae'}
MY_MEASURES = {'Gini@10:', 'Gini@100:', 'MSI@10:', 'MSI@100:'}
TREC_PREFIX = 'trec-'
EVAL_PREFIX = 'eval-'
FOLD_ONE = "fold1.txt"


def parse_sub_trec(trec):
    # Initialize measures dict
    result = {}
    for measure in TREC_MEASURES:
        result[measure] = []
    for line in trec:
        if 'all' not in line:  # Read users metrics, not summary metrics
            measure, _, value = line.split()
            if measure in TREC_MEASURES:
                result[measure].append(float(value))
    return result


def parse_trec(trec):
    result = {}
    for line in trec:
        if 'all' in line:
            measure, _, value = line.split()
            if measure in TREC_MEASURES:
                result[measure] = float(value)
    return result


def parse_metric(metric):
    result = {}
    for line in metric:
        measure, value = line.split()
        if measure in MY_MEASURES:
            result[measure] = float(value)
    return result


def get_measures(trecs_folder, metrics_folder, filename):
    with open(join(trecs_folder, TREC_PREFIX + filename), 'r') as trec:
        measures = parse_trec(trec)
    if metrics_folder is not None:
        with open(join(metrics_folder, EVAL_PREFIX + filename), 'r') as metric:
            measures.update(parse_metric(metric))
    return measures


def get_measures_sub_trec(trecs_folder, filename):
    with open(join(trecs_folder, filename), 'r') as trec:
        measures = parse_sub_trec(trec)
    return measures


def get_trec_avg(sub_trecs_folder, trecs_folder, run_name):
    output_path = join(trecs_folder, run_name + ".txt")
    if not isfile(output_path):
        tmp_output_path = output_path[:-4] + ".tmp"
        # Get subtrecs files for this run
        sub_trecs = [f for f in listdir(sub_trecs_folder) if isfile(join(sub_trecs_folder, f))]
        measures = []
        # Read measures from each subtrec
        try:
            for sub_trec in sub_trecs:
                measures.append(get_measures_sub_trec(sub_trecs_folder, sub_trec))
        except (KeyError, ValueError) as exc:
            print(exc)
            return None
        # Get avg and write to complete trec
        with open(tmp_output_path, 'w+') as trecs:
            for j in sorted(measures[0]):
                value = 0.0
                denominator = 0
                for i in range(0, len(sub_trecs)):
                    try:
                        value += sum(measures[i][j])
                        denominator += len(measures[i][j])
                    except Exception as exc:
                        raise AttributeError(
                            str(exc) + " in subtrec " + sub_trecs_folder)
                value /= float(denominator)
                trecs.write("{0}\tall\t{1:.5f}\n".format(j, value))

        rename(tmp_output_path, output_path)


def get_avg(folds, trecs_folder, metrics_folder, filename):
    files = {}
    for i in folds:
        files[i] = "{0}fold{1}.txt".format(filename, i)
    measures = []
    output = filename[:-1]
    try:
        for j in files:
            measures.append(get_measures(
                trecs_folder, metrics_folder, files[j]))
    except (KeyError, ValueError) as exc:
        return None

    for j in sorted(measures[0]):
        value = 0.0
        for i in range(0, len(folds)):
            try:
                value += measures[i][j]
            except Exception as exc:
                raise AttributeError(
                    str(exc) + " in " + filename + "fold" + str(i + 1))
        value /= float(len(folds))
        output += ",{0}".format(value)

    return output


def run_trec(folds, trecs_folder):
    # Get all trecs folders (each trec is divided in subtrecs stored in subtrecs folders)
    folders = [f for f in listdir(trecs_folder) if isdir(join(trecs_folder, f))]
    args = []

    for folder in folders:
        regexp = re.match(r""".*fold(?P<fold>[0-9]+)""", folder)

        try:
            fold = int(regexp.group("fold"))
        except AttributeError as exc:
            raise AttributeError(str(exc) + " in " + folder)
        folds.add(fold)
        # Get all subtrecs for this subtrec folder
        sub_trecs_folder = join(trecs_folder, folder)
        args.append((sub_trecs_folder, trecs_folder, folder))

    with Pool(processes=1) as pool:
        pool.starmap(get_trec_avg, args)
        pool.close()
        pool.join()


def print_header(should_print_my_measures=False):
    sys.stdout.write("Recommender")
    measures = TREC_MEASURES | MY_MEASURES if bool(
        should_print_my_measures) else TREC_MEASURES
    for measure in sorted(measures):
        sys.stdout.write(",{0}".format(measure))
    sys.stdout.write("\n")


def print_results(folds, trecs_folder, metrics_folder):
    files = [f.replace(TREC_PREFIX, "") for f in listdir(trecs_folder)
             if isfile(join(trecs_folder, f)) and f.endswith(FOLD_ONE)]
    files.sort()
    print_header(metrics_folder)
    with Pool(processes=1) as pool:
        args = [(folds, trecs_folder, metrics_folder, f[:-len(FOLD_ONE)])
                for f in files]
        for result in pool.starmap(get_avg, args):
            if result is not None:
                print(result)
        pool.close()
        pool.join()


def create_if_necessary(folder):
    if not isdir(folder):
        if exists(folder):
            print("{0} already exists and it is not a folder".format(folder))
        else:
            makedirs(folder)


def main():
    if len(sys.argv) < 2:
        print("{0} <trecs_folder> [<metrics_folder>]"
              .format(sys.argv[0]))
        sys.exit(0)

    trecs_folder = sys.argv[1]
    metrics_folder = sys.argv[2] if len(sys.argv) > 2 else None

    folds = set()

    create_if_necessary(trecs_folder)
    run_trec(folds, trecs_folder)
    print_results(folds, trecs_folder, metrics_folder)


if __name__ == '__main__':
    main()
