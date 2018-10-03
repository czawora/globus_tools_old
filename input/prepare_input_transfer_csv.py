import os
import csv
import re
import glob
import argparse
import datetime


# def make_lfp_pipeline_command(subj_path, nsx_ext, analog_pulse_ext):
#
# 	return("python3 /data/zaworaca/data/python_scripts/lfp/construct_lfp_bash_scripts.py " + subj_path + " " + nsx_ext + " " + analog_pulse_ext)
#
#
# def make_spike_pipeline_command(subj_path, input_channels, nsx_ext, analog_pulse_ext):
#
# 	return("python3 /data/zaworaca/data/python_scripts/mixchan_reref/construct_bash_scripts.py " + subj_path + " " + input_channels + " " + nsx_ext + " " + analog_pulse_ext)


if __name__ == "__main__":

	digital_pulse = "nev"

	use_backup_analog = True
	use_backup_digital = True

	upload_flag_file = "complete.complete"

	abspath = os.path.abspath(__file__)
	dname = os.path.dirname(abspath)
	os.chdir(dname)

	input_batch_file = "input_batch.txt"
	input_bash_file = "input_transfer.sh"
	input_batch_archive = "_input_script_archive"
	input_create_pipeline_script = "make_pipelines.sh"

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

	parser = argparse.ArgumentParser(description='build list of dir + files to transfer to biowulf, also write transfer script')

	parser.add_argument('biowulf_dest_path')
	parser.add_argument('sesslist')

	# parser.add_argument('--mem_limit_gb', default = 0, type = int)
	# parser.add_argument('--local_output_dir', default = 'biowulf')
	parser.add_argument('--dry_run', action='store_true')

	args = parser.parse_args()

	biowulf_dest_path = args.biowulf_dest_path
	sesslist = args.sesslist
	# mlimit = args.mem_limit_gb
	# local_output_dir = args.local_output_dir
	dry_run = args.dry_run
	mlimit = 0

	datestring_regex = re.compile(r'.*(\d\d\d\d\d\d_\d\d\d\d).*')
	subjstring_regex = re.compile(r'.*(NIH\d\d\d).*')

	def takeDate(elem):
		return((elem["date_YMD"], elem["date_HM"]))

	if os.path.isfile(sesslist) is False:
		print(sesslist + " is not a valid filepath.")
		exit(-1)

	uploaded_sessions = []

	# read the csv
	sesslist_csv_rows = []

	sesslist_f = open(sesslist)
	sesslist_csv = csv.reader(sesslist_f, delimiter=",")

	sesslist_csv_rows = [l for l in sesslist_csv]

	sesslist_f.close()

	# read the rows of the csv file
	recording_filesets = []

	for csv_row in sesslist_csv_rows:

		current_sess_path = csv_row[0]
		current_sess_nsp = csv_row[1]
		current_sess_analog_ext = csv_row[2]
		current_sess_physio_ext = csv_row[3]

		# check for good session name
		sessname_match = re.match(datestring_regex, current_sess_path)

		if sessname_match is not None:

			datestring_match = re.findall(datestring_regex, current_sess_path)[0]
			datestring_YMD = datestring_match.split("_")[0]
			datestring_HM = datestring_match.split("_")[1]

			subjname = re.findall(subjstring_regex, current_sess_path)[0]

			session_fileset = {}

			session_fileset.update({"date_YMD": datestring_YMD})
			session_fileset.update({"date_HM": datestring_HM})

			session_fileset.update({"subj": subjname})
			session_fileset.update({"session_path": current_sess_path})
			session_fileset.update({"session_name": current_sess_path.split("/")[-1]})

			session_fileset.update({"ns6": ""})
			session_fileset.update({"ns6_filesize": 0})

			session_fileset.update({"ns5": ""})
			session_fileset.update({"ns5_filesize": 0})

			session_fileset.update({"analog_pulse": ""})
			session_fileset.update({"analog_pulse_filesize": 0})

			session_fileset.update({"digital_pulse": ""})
			session_fileset.update({"digital_pulse_filesize": 0})

			# current_sess_path_filestub = current_sess_path + "/" + datestring_match + "_" + current_sess_nsp
			physio_glob = glob.glob(current_sess_path + "/*." + current_sess_physio_ext)
			analog_pulse_glob = glob.glob(current_sess_path + "/*." + current_sess_analog_ext)
			digital_pulse_glob = glob.glob(current_sess_path + "/*." + digital_pulse)

			if physio_glob != []:

				for found_file in physio_glob:

					if current_sess_nsp in found_file:

						filesize = os.path.getsize(found_file)

						if current_sess_physio_ext == "ns5":

							session_fileset["ns5"] = found_file.split("/")[-1]
							session_fileset["ns5_filesize"] = filesize

						elif current_sess_physio_ext == "ns6":

							session_fileset["ns6"] = found_file.split("/")[-1]
							session_fileset["ns6_filesize"] = filesize

						else:
							print("physio_ext entered for session '" + current_sess_path + "' is neither ns5 nor ns6")

			if current_sess_analog_ext != "None" and analog_pulse_glob != []:

				for found_file in analog_pulse_glob:

					if current_sess_nsp in found_file:

						filesize = os.path.getsize(found_file)
						session_fileset["analog_pulse"] = found_file.split("/")[-1]
						session_fileset["analog_pulse_filesize"] = filesize

				if session_fileset["analog_pulse"] == "" and use_backup_analog:
					# there was no analog pulse file with the specified combo of regex + ext found in the folder
					# in this case, the glob only contains at most one other file with the chosen extension, so take it

					if len(analog_pulse_glob) > 1:
						print("the day has come! length of analog pulse glob is > 1 when looking for a backup file from the other nsp. Edit code to handle this case.")
						exit(-10)

					found_file = analog_pulse_glob[0]

					filesize = os.path.getsize(found_file)
					session_fileset["analog_pulse"] = found_file.split("/")[-1]
					session_fileset["analog_pulse_filesize"] = filesize

			if digital_pulse != "None" and digital_pulse_glob != []:

				for found_file in digital_pulse_glob:

					if current_sess_nsp in found_file:

						filesize = os.path.getsize(found_file)
						session_fileset["digital_pulse"] = found_file.split("/")[-1]
						session_fileset["digital_pulse_filesize"] = filesize

				if session_fileset["digital_pulse"] == "" and use_backup_digital:
					# there was no digital pulse file with the specified combo of regex + ext found in the folder
					# in this case, the glob is non-empty and only contains at most one other file with the chosen extension, so take it

					if len(digital_pulse_glob) > 1:
						print("the day has come! length of digital pulse glob is > 1 when looking for a backup file from the other nsp. Edit code to handle this case.")
						exit(-10)

					found_file = digital_pulse_glob[0]

					filesize = os.path.getsize(found_file)
					session_fileset["digital_pulse"] = found_file.split("/")[-1]
					session_fileset["digital_pulse_filesize"] = filesize

			session_fileset.update({"session_filesize":  session_fileset["ns6_filesize"] + session_fileset["ns5_filesize"] + session_fileset["analog_pulse_filesize"] + session_fileset["digital_pulse_filesize"]})
			recording_filesets.append(session_fileset)

	recording_filesets = sorted(recording_filesets, key=takeDate)

	not_uploaded_sessions = [s for s in recording_filesets if s["session_name"] not in uploaded_sessions and s["session_filesize"] != 0]
	not_uploaded_session_names = [s["session_name"] for s in recording_filesets if s["session_name"] not in uploaded_sessions and s["session_filesize"] != 0]

	########################################################################################################
	########################################################################################################

	if dry_run:

		print("found the following files")

		uploaded_size = 0
		not_uploaded_size = 0

		for sess in recording_filesets:

			if sess["session_name"] not in not_uploaded_session_names:

				uploaded_size += sess["session_filesize"]

			if sess["session_name"] in not_uploaded_session_names:

				not_uploaded_size += sess["session_filesize"]
				print(sess["subj"] + "--" + sess["session_name"] + " ( " + str(sess["session_filesize"]/1e9) + " Gb )  -- NOT YET UPLOADED")

		print(str(len(uploaded_sessions)) + " sessions ( " + str(uploaded_size/1e9) + " Gb / " + str(uploaded_size/1e12) + " Tb ) from those listed above have been uploaded to biowulf")
		print(str(len(not_uploaded_sessions)) + " sessions ( " + str(not_uploaded_size/1e9) + " Gb / " + str(not_uploaded_size/1e12) + " Tb ) from this subj_path have yet to be uploaded to biowulf")

		exit(0)

	########################################################################################################
	########################################################################################################
	# make list of files that will be transferred within memory limit

	current_upload_list = []
	mem_count = 0

	if mlimit == 0:

		current_upload_list = not_uploaded_sessions
		mem_count = sum([sess["session_filesize"] for sess in not_uploaded_sessions])/1e9

	else:

		for sess in not_uploaded_sessions:

			current_session_size_gb = sess["session_filesize"]/1e9

			if sess["session_filesize"] != 0 and (mem_count + current_session_size_gb) < mlimit:

				current_upload_list.append(sess)
				mem_count += current_session_size_gb

	if mlimit == 0:
		print("memory limit is boundless (0), this allows for " + str(len(current_upload_list)) + " sessions ( " + str(mem_count) + " Gb ) to be uploaded ( of " + str(len(not_uploaded_sessions)) + " )")
	else:
		print("memory limit is " + str(mlimit) + " Gb, this allows for " + str(len(current_upload_list)) + " sessions ( " + str(mem_count) + " Gb ) to be uploaded ( of " + str(len(not_uploaded_sessions)) + " )")

	########################################################################################################
	########################################################################################################
	# archive old transfer scripts

	run_timestamp = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

	if os.path.isfile(input_batch_file):

		existing_batch = open(input_batch_file)

		# skip the # comment char
		existing_timestamp = existing_batch.readline()[1:].strip("\n")

		if os.path.isdir(input_batch_archive) is False:
			os.mkdir(input_batch_archive)

		os.rename(input_batch_file, input_batch_archive + "/" + existing_timestamp + "_batch.txt")

		existing_batch.close()

		os.rename(input_bash_file, input_batch_archive + "/" + existing_timestamp + "_transfer.sh")

	########################################################################################################
	########################################################################################################
	# prepare transfer scripts

	transfer_count = len(current_upload_list)

	new_bash = open(input_bash_file, 'w')

	new_bash.write("#!/usr/bin/bash\n\n")

	# for sess in current_upload_list:
	#
	# 	current_mkdir = biowulf_dest_path + "/" + sess["subj"] + "/" + sess["session_name"]
	#
	# 	new_bash.write("if [ ! -d " + current_mkdir + " ]; then\n")
	# 	new_bash.write("mkdir -p -m 777 " + current_mkdir + "\n")
	# 	new_bash.write("fi\n")

	# now write the transfer bash command
	new_bash.write("globus transfer ")
	new_bash.write(os.environ["FRNU56_GLOBUS"])
	new_bash.write(" ")
	new_bash.write(os.environ["NIH_GLOBUS"])
	new_bash.write(" --no-verify-checksum ")
	new_bash.write(" --sync-level size ")
	new_bash.write(" --batch --label \"")
	new_bash.write(run_timestamp + " transfer-" + str(transfer_count))
	new_bash.write("\" < " + input_batch_file.split("/")[-1])

	new_bash.close()

	new_batch = open(input_batch_file, 'w')

	# write timestamp
	new_batch.write("#" + run_timestamp + "\n")
	new_batch.write("#transfer count: " + str(transfer_count) + "\n")
	new_batch.write("\n\n")

	for sess in current_upload_list:

		current_dest_subj_path = biowulf_dest_path + "/" + sess["subj"]
		current_dest_sess_path = biowulf_dest_path + "/" + sess["subj"] + "/" + sess["subj"] + "_" + sess["session_name"]

		if sess["ns6"] != "":

			new_batch.write(sess["session_path"] + "/" + sess["ns6"])
			new_batch.write(" ")
			new_batch.write(current_dest_sess_path + "/" + sess["subj"] + "_" + sess["ns6"])
			new_batch.write("\n")

		if sess["ns5"] != "":

			new_batch.write(sess["session_path"] + "/" + sess["ns5"])
			new_batch.write(" ")
			new_batch.write(current_dest_sess_path + "/" + sess["subj"] + "_" + sess["ns5"])
			new_batch.write("\n")

		if sess["analog_pulse"] != "":

			new_batch.write(sess["session_path"] + "/" + sess["analog_pulse"])
			new_batch.write(" ")
			new_batch.write(current_dest_sess_path + "/" + sess["subj"] + "_" + sess["analog_pulse"])
			new_batch.write("\n")

		if sess["digital_pulse"] != "":

			new_batch.write(sess["session_path"] + "/" + sess["digital_pulse"])
			new_batch.write(" ")
			new_batch.write(current_dest_sess_path + "/" + sess["subj"] + "_" + sess["digital_pulse"])
			new_batch.write("\n")

	new_batch.close()

	print("\n\n")
	print("biowulf_dest_path: " + biowulf_dest_path)
	print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^  -- MAKE SURE THIS IS RIGHT\n\n")
	print("*** run the bash script created here on biowulf")
