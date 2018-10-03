
import os
import subprocess
import datetime
import argparse
import glob

abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

output_batch_file = "output_batch.txt"
output_bash_file = "output_transfer.sh"
output_batch_archive = "_output_script_archive"

print("*** you SHOULD be running this on biowulf")
print("*** make sure globus-cli is installed and findable in PATH -- https://docs.globus.org/cli/")
print("*** use 'globus whoami' to check your current globus login identity | use 'globus login' to login")


########################################################################################################
########################################################################################################
# make sure environment varibales are in place

if "FRNU56_GLOBUS" not in os.environ:

    print("FRNU56_GLOBUS env variable is missing")

    resp = input("set FRNU56_GLOBUS to '32749cf0-0537-11e8-a6a1-0a448319c2f8'? ")

    if resp.lower() == "y":
        os.environ["FRNU56_GLOBUS"] = "32749cf0-0537-11e8-a6a1-0a448319c2f8"

if "NIH_GLOBUS" not in os.environ:

    print("NIH_GLOBUS env variable is missing")

    resp = input("set NIH_GLOBUS to 'e2620047-6d04-11e5-ba46-22000b92c6ec'? ")

    if resp.lower() == "y":
        os.environ["NIH_GLOBUS"] = "e2620047-6d04-11e5-ba46-22000b92c6ec"


########################################################################################################
########################################################################################################


# print(["globus", "ls", os.environ["NIH_GLOBUS"] + ":~"])
#
# globus_login_check = str(subprocess.check_output(["globus", "ls", os.environ["NIH_GLOBUS"] + ":/~"]))
#
# if "Globus CLI Error" in globus_login_check:
#     print("globus login check: FAIL, need to 'globus login' first")
#     exit(1)
# else:
#     print("globus login check: GOOD")

# globus_login_check = str(subprocess.check_output(["globus", "ls", os.environ["FRNU56_GLOBUS"] + ":/~"]))

# if "Globus CLI Error" in globus_login_check:
# 	print("globus login check for endpoint FRNU56_GLOBUS: FAIL, need to 'globus login' first")
# 	exit(1)
# else:
# 	print("globus login check for endpoint FRNU56_GLOBUS: GOOD")


# arg parse
parser = argparse.ArgumentParser(description='build list of dir + files to transfer from biowulf, also write transfer script')

parser.add_argument('--transfer_dirs', nargs="+")
parser.add_argument('--sources', nargs="+")

args = parser.parse_args()

transfer_dirs = args.transfer_dirs
srcs = args.sources

if transfer_dirs is None or srcs is None:

    print("at least one argument is required for --transfer_dirs and --sources")
    exit(1)


if len(transfer_dirs) != len(srcs):
    print("provide a transfer directory for every source given")
    exit(1)


run_timestamp = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

# "subj" level folders, will look in each for session folders
for src in srcs:
    print("source = " + src)

# archive existing batch file
if os.path.isfile(output_batch_file):

    existing_batch = open(output_batch_file)

    # skip the # comment char
    existing_timestamp = existing_batch.readline()[1:].strip("\n")

    if os.path.isdir(output_batch_archive) is False:
        os.mkdir(output_batch_archive)

    os.rename(output_batch_file, output_batch_archive + "/" + existing_timestamp + "_batch.txt")

    existing_batch.close()

    os.rename(output_bash_file, output_batch_archive + "/" + existing_timestamp + "_transfer.sh")


# create new bash
new_bash = open(output_bash_file, 'w')
new_bash.write("#!/usr/bin/bash\n")

# create new batch
new_batch = open(output_batch_file, 'w')

# write timestamp
new_batch.write("#" + run_timestamp + "\n\n")

transfer_count = 0

# add transfers
for idx, src in enumerate(srcs):

    current_transfer_dir = transfer_dirs[idx]

    print("scanning source: " + src)
    print("planning to transfer to: " + current_transfer_dir)

    # note the src
    new_batch.write("#src = " + src + "\n")

    src_splits = src.split("/")

    if src_splits[-1] == "":
        src_name = src_splits[-2]
    else:
        src_name = src_splits[-1]

    src_glob = glob.glob(src + "/*/spike/outputs") + glob.glob(src + "/*/spike/_ignore_me.txt")
    # src_glob = glob.glob(src + "/*/*spikeInfo.mat") + glob.glob(src + "/*/_ignore_me.txt")

    sort_src_sessions = list(set( [ f.split("/spike")[0] for f in src_glob ] ))

    for sess in sort_src_sessions:

        print(sess)

        dest_sess_level = current_transfer_dir + "/" + sess.split("/")[-1]

        # check for ignore_mes
        for f in glob.glob(sess + "/spike/_ignore_me.txt"):

            fname = f.split("/")[-1]

            new_batch.write(sess + "/spike/" + fname)
            new_batch.write(" ")
            new_batch.write(dest_sess_level + "/" + fname)
            new_batch.write("\n")
            transfer_count += 1

        # check for spikeInfos
        for f in glob.glob(sess + "/spike/outputs/*"):

            fname = f.split("/")[-1]

            if os.path.isdir(f):
                new_batch.write(" --recursive ")

            new_batch.write(sess + "/spike/outputs/" + fname)
            new_batch.write(" ")
            new_batch.write(dest_sess_level + "/" + fname)
            new_batch.write("\n")
            transfer_count += 1

        # new_bash.write("if [ ! -d \"" + dest_sess_level + "\" ]; then\n")
        # new_bash.write("mkdir -p " + dest_sess_level + "\n")
        # new_bash.write("fi\n")

    # look for lfp results
    src_glob = glob.glob(src + "/*/lfp/outputs")

    lfp_src_sessions = list(set( [ f.split("/lfp")[0] for f in src_glob ] ))

    for sess in lfp_src_sessions:

        print(sess)

        dest_sess_level = current_transfer_dir + "/" + sess.split("/")[-1]

        # check for spikeInfos
        for f in glob.glob(sess + "/lfp/outputs/*"):

            fname = f.split("/")[-1]

            if os.path.isdir(f):
                new_batch.write(" --recursive ")

            new_batch.write(sess + "/lfp/outputs/" + fname)
            new_batch.write(" ")
            new_batch.write(dest_sess_level + "/" + fname)
            new_batch.write("\n")
            transfer_count += 1

        # # if the session has spike outputs, this bash command would have been written above
        # if sess not in sort_src_sessions:
        #
        #     new_bash.write("if [ ! -d \"" + dest_sess_level + "\" ]; then\n")
        #     new_bash.write("mkdir -p " + dest_sess_level + "\n")
        #     new_bash.write("fi\n")

# now write the tranfer bash command
new_bash.write("globus transfer ")
new_bash.write(os.environ["NIH_GLOBUS"])
new_bash.write(" ")
new_bash.write(os.environ["FRNU56_GLOBUS"])
new_bash.write(" --no-verify-checksum ")
new_bash.write(" --batch --label \"")
new_bash.write(run_timestamp + " transfer-" + str(transfer_count))
new_bash.write("\" < " + output_batch_file)

new_bash.close()
new_batch.close()


        # # check for spikeWaveform
        # for f in glob.glob(sess + "/*spikeWaveform.mat"):
        #
        #     fname = f.split("/")[-1]
        #
        #     new_batch.write(sess + "/" + fname)
        #     new_batch.write(" ")
        #     new_batch.write(dest_sess_level + "/" + fname)
        #     new_batch.write("\n")
        #     transfer_count += 1
        #
        # # check for summary.csv
        # for f in glob.glob(sess + "/*_summary.csv"):
        #
        #     fname = f.split("/")[-1]
        #
        #     new_batch.write(sess + "/" + fname)
        #     new_batch.write(" ")
        #     new_batch.write(dest_sess_level + "/" + fname)
        #     new_batch.write("\n")
        #     transfer_count += 1
        #
        # # check for chans.csv
        # for f in glob.glob(sess + "/*_chans.csv"):
        #
        #     fname = f.split("/")[-1]
        #
        #     new_batch.write(sess + "/" + fname)
        #     new_batch.write(" ")
        #     new_batch.write(dest_sess_level + "/" + fname)
        #     new_batch.write("\n")
        #     transfer_count += 1
        #
        # # check for sortFigs
        # for f in glob.glob(sess + "/sortFigs"):
        #
        #     fname = f.split("/")[-1]
        #
        #     new_batch.write("--recursive ")
        #     new_batch.write(sess + "/" + fname)
        #     new_batch.write(" ")
        #     new_batch.write(dest_sess_level + "/" + fname)
        #     new_batch.write("\n")
        #     transfer_count += 1
