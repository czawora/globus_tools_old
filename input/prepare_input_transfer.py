#!/usr/bin/python3

import sys
import csv
import os
import subprocess
import datetime
import argparse
import glob
import re


########################################################################################################
########################################################################################################
# helper functions

def parse_regex(regex_str):

	return_string = regex_str

	#replace \\ with \
	return_string = return_string.replace("\\\\", "\\")

	return(return_string)

########################################################################################################
########################################################################################################

if __name__ == "__main__":

	upload_flag_file = "complete.complete"

	abspath = os.path.abspath(__file__)
	dname = os.path.dirname(abspath)
	os.chdir(dname)

	input_batch_file = "input_batch.txt"
	input_bash_file =  "input_transfer.sh"
	input_batch_archive =  "_input_script_archive"


	# print("*** make sure globus-cli is installed and findable in PATH -- https://docs.globus.org/cli/")
	# print("*** use 'globus whoami' to check your current globus login identity | use 'globus login' to login")
	#

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
	# make sure globus connection works


	# globus_login_check = str(subprocess.check_output(["globus", "ls", os.environ["NIH_GLOBUS"] + ":/~"]))
	#
	# if "Globus CLI Error" in globus_login_check:
	# 	print("globus login check for endpoint NIH_GLOBUS: FAIL, need to 'globus login' first")
	# 	exit(1)
	# else:
	# 	print("globus login check for endpoint NIH_GLOBUS: GOOD")
	#
	#
	#
	# globus_login_check = str(subprocess.check_output(["globus", "ls", os.environ["FRNU56_GLOBUS"] + ":/~"]))
	#
	# if "Globus CLI Error" in globus_login_check:
	# 	print("globus login check for endpoint FRNU56_GLOBUS: FAIL, need to 'globus login' first")
	# 	exit(1)
	# else:
	# 	print("globus login check for endpoint FRNU56_GLOBUS: GOOD")
	#
	# print("\n\n")

	########################################################################################################
	########################################################################################################

	parser = argparse.ArgumentParser(description = 'build list of dir + files to transfer to biowulf, also write transfer script')

	parser.add_argument('subj_path')
	parser.add_argument('biowulf_dest_path')
	parser.add_argument('--fname_regex', default = '.*')
	parser.add_argument('--raw_dir', default = 'data_raw')

	parser.add_argument('--date_start', default="inf")
	parser.add_argument('--date_end', default="inf")

	parser.add_argument('--ns5', action='store_true')
	parser.add_argument('--ns6', action='store_true')

	parser.add_argument('--analog_pulse', default = 'None', nargs = '?', choices = ['ns2', 'ns3', 'ns4', 'ns5', 'ns6', 'None'], help = 'which file extension in the session folder contains the analog pulses, or None to ignore')
	parser.add_argument('--digital_pulse', default = 'nev', nargs = '?', choices = ['nev', 'None'], help = 'which file extension in the session folder contains the digital pulses, or None to ignore')

	parser.add_argument('--mem_limit_gb', default = 0, type = int)
	parser.add_argument('--local_output_dir', default = 'biowulf')
	parser.add_argument('--dry_run', action='store_true')

	args = parser.parse_args()

	subj_path = args.subj_path
	biowulf_dest_path = args.biowulf_dest_path
	fname_regex = args.fname_regex
	raw_dir = args.raw_dir

	target_date_start = args.date_start
	target_date_end = args.date_end

	ns5 = args.ns5
	ns6 = args.ns6

	analog_pulse = args.analog_pulse
	digital_pulse = args.digital_pulse

	use_backup_analog = True
	use_backup_digital = True

	mlimit = args.mem_limit_gb
	local_output_dir = args.local_output_dir
	dry_run = args.dry_run

	session_path = subj_path + "/" + raw_dir
	output_path = subj_path + "/" + local_output_dir

	# parse and validate input regex

	regex_str = parse_regex(fname_regex)
	re_pattern = re.compile(regex_str)

	validate_input_datetime_regex = re.compile(r'\d\d\d\d\d\d_\d\d\d\d')


	if target_date_start is "inf":

		target_start_datetime = datetime.datetime.min

	elif re.match(validate_input_datetime_regex, target_date_start) is not None:

		target_start_datetime = datetime.datetime( int("20"+target_date_start[0:2]), int(target_date_start[2:4]), int(target_date_start[4:6]), hour=int(target_date_start[0:2]), minute=int(target_date_start[2:4]))

	else:

		print("--date_start must be in the format of YYMMDD_hhmm")
		exit(1)


	if target_date_end is "inf":

		target_end_datetime = datetime.datetime.max

	elif re.match(validate_input_datetime_regex, target_date_end) is not None:

		target_end_datetime = datetime.datetime( int("20"+target_date_end[0:2]), int(target_date_end[2:4]), int(target_date_end[4:6]), hour=int(target_date_end[0:2]), minute=int(target_date_end[2:4]))

	else:

		print("--date_end must be in the format of YYMMDD_hhmm")
		exit(1)

	########################################################################################################
	########################################################################################################
	# make sure paths from arguments exist

	if os.path.isdir(subj_path) is False:
		print("value passed as --subj_path is not a valid path")
		exit(-1)

	if os.path.isdir(session_path) is False:
		print("value passed as --raw_dir does not lead to a valid path")
		exit(-2)

	# #make output dir if it doesnt exist
	# if local_output_dir != "None" and os.path.isdir(output_path) == False:
	# 	os.mkdir(output_path)


	########################################################################################################
	########################################################################################################
	# gather list of already transferred files

	uploaded_sessions = []
	#
	# if local_output_dir != "None":
	#
	# 	output_dir_ls = os.listdir(output_path)
	# else:
	#
	# 	output_dir_ls = []
	#
	# for f in output_dir_ls:
	#
	# 	if f[0] != "_" and os.path.isdir(f):
	#
	# 		temp_output_path = subj_path + "/" + f
	#
	# 		if os.path.isfile(temp_output_path + "/" + upload_flag_file):
	#
	# 			uploaded_files.append(f)


	########################################################################################################
	########################################################################################################
	# gather list of files in raw_dir
	# xpecting files folders to have dddddd_dddd.*
	def takeDate(elem):
		return((elem["date_YMD"], elem["date_HM"]))

	datestring_regex = re.compile(r'(\d\d\d\d\d\d_\d\d\d\d).*')

	recording_filesets = []

	session_path_ls = os.listdir(session_path)

	for sess in session_path_ls:

		# check for good session name
		sessname_match = re.match(datestring_regex, sess)

		if sessname_match is not None:

			datestring_match = re.findall(datestring_regex, sess)[0]
			datestring_YMD = datestring_match.split("_")[0]
			datestring_HM = datestring_match.split("_")[1]

			# create session datetime object
			sess_datetime = datetime.datetime(int("20"+datestring_YMD[0:2]), int(datestring_YMD[2:4]), int(datestring_YMD[4:6]), hour=int(datestring_HM[0:2]), minute=int(datestring_HM[2:4]))

			# good session name, check for goood filename
			current_sess_path = session_path + "/" + sess

			sess_dir_ls = os.listdir(current_sess_path)

			session_unique_filenames = list(set([f[:-4] for f in sess_dir_ls]))

			for f in session_unique_filenames:

				session_fileset = {}

				session_fileset.update({"date_YMD": datestring_YMD})
				session_fileset.update({"date_HM": datestring_HM})

				session_fileset.update({"session_path": current_sess_path})
				session_fileset.update({"session_name": f})

				session_fileset.update({"ns6": ""})
				session_fileset.update({"ns6_filesize": 0})

				session_fileset.update({"ns5": ""})
				session_fileset.update({"ns5_filesize": 0})

				session_fileset.update({"analog_pulse": ""})
				session_fileset.update({"analog_pulse_filesize": 0})

				session_fileset.update({"digital_pulse": ""})
				session_fileset.update({"digital_pulse_filesize": 0})

				if re.match(re_pattern, f) is not None and sess_datetime >= target_start_datetime and sess_datetime <= target_end_datetime:

					current_sess_path_prefix = current_sess_path + "/" + f

					print("match: " + f)

					ns6_glob = glob.glob(current_sess_path_prefix + ".ns6")
					ns5_glob = glob.glob(current_sess_path_prefix + ".ns5")
					analog_pulse_glob = glob.glob(current_sess_path_prefix + "." + analog_pulse)
					digital_pulse_glob = glob.glob(current_sess_path_prefix + "." + digital_pulse)

					if ns6 and ns6_glob != []:

						for found_file in ns6_glob:

							filesize = os.path.getsize(found_file)
							session_fileset["ns6"] = found_file.split("/")[-1]
							session_fileset["ns6_filesize"] = filesize

					if ns5 and ns5_glob != []:

						for found_file in ns5_glob:

							filesize = os.path.getsize(found_file)
							session_fileset["ns5"] = found_file.split("/")[-1]
							session_fileset["ns5_filesize"] = filesize

					if analog_pulse != "None" and analog_pulse_glob != []:

						for found_file in analog_pulse_glob:

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

				session_fileset.update({"session_filesize":  session_fileset["ns6_filesize"] + session_fileset["ns5_filesize"] + session_fileset["analog_pulse_filesize"] + session_fileset["digital_pulse_filesize"]} )

				recording_filesets.append(session_fileset)

	recording_filesets = sorted(recording_filesets, key=takeDate)

	not_uploaded_sessions = [s for s in recording_filesets if s["session_name"] not in uploaded_sessions and s["session_filesize"] != 0]
	not_uploaded_session_names = [s["session_name"] for s in recording_filesets if s["session_name"] not in uploaded_sessions and s["session_filesize"] != 0]

	########################################################################################################
	########################################################################################################
	# dry run stats

	if dry_run:

		print("found the following files in " + subj_path)

		uploaded_size = 0
		not_uploaded_size = 0

		for sess in recording_filesets :

			if sess["session_name"] not in not_uploaded_session_names:

				uploaded_size += sess["session_filesize"]

			if sess["session_name"] in not_uploaded_session_names:

				not_uploaded_size += sess["session_filesize"]
				print(sess["session_name"] + " ( " + str(sess["session_filesize"]/1e9) + " Gb )  -- NOT YET UPLOADED")


		print(str(len(uploaded_sessions)) + " sessions ( " + str(uploaded_size/1e9) + " Gb / " + str(uploaded_size/1e12) + " Tb ) from those listed above have been uploaded to biowulf")
		print(str(len(not_uploaded_sessions)) + " sessions ( " + str(not_uploaded_size/1e9) + " Gb / " + str(not_uploaded_size/1e12) + " Tb ) from this subj_path have yet to be uploaded to biowulf")

		exit(0)

	########################################################################################################
	########################################################################################################
	# make list of files that will be transferred within memory limit

	current_upload_list = []
	mem_count = 0

	if mlimit == 0:

		current_upload_list =  not_uploaded_sessions
		mem_count = sum([ sess["session_filesize"] for sess in not_uploaded_sessions ])/1e9

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
	#archive old transfer scripts

	run_timestamp = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

	if os.path.isfile(input_batch_file):

		existing_batch = open(input_batch_file)

		#skip the # comment char
		existing_timestamp = existing_batch.readline()[1:].strip("\n")

		if os.path.isdir(input_batch_archive) == False:
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
	# 	current_mkdir = biowulf_dest_path + "/" + sess["session_name"]
	#
	# 	new_bash.write("if [ ! -d " + current_mkdir + " ]; then\n")
	# 	new_bash.write("mkdir -p -m 777 " + current_mkdir + "\n")
	# 	new_bash.write("fi\n")

	#now write the transfer bash command
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

	#write timestamp
	new_batch.write("#" + run_timestamp + "\n")
	new_batch.write("#subj_path: " + subj_path + "\n")
	new_batch.write("#regex: " + fname_regex + "\n")
	new_batch.write("#raw_dir: " + raw_dir + "\n")
	new_batch.write("#date_start: " + target_date_start + "\n")
	new_batch.write("#date_end: " + target_date_end + "\n")
	new_batch.write("#ns6: " + str(ns6) + " | ns5: " + str(ns5)  + " | analog pulse: " + analog_pulse + " | digital pulse: " + digital_pulse + "\n")
	new_batch.write("#transfer count: " + str(transfer_count) + "\n")
	new_batch.write("\n\n")

	for sess in current_upload_list:

		if sess["ns6"] != "":

			new_batch.write(sess["session_path"] + "/" + sess["ns6"])
			new_batch.write(" ")
			new_batch.write(biowulf_dest_path + "/" + sess["session_name"] + "/" + sess["ns6"])
			new_batch.write("\n")

		if sess["ns5"] != "":

			new_batch.write(sess["session_path"] + "/" + sess["ns5"])
			new_batch.write(" ")
			new_batch.write(biowulf_dest_path + "/" + sess["session_name"] + "/" + sess["ns5"])
			new_batch.write("\n")

		if sess["analog_pulse"] != "":

			new_batch.write(sess["session_path"] + "/" + sess["analog_pulse"])
			new_batch.write(" ")
			new_batch.write(biowulf_dest_path + "/" + sess["session_name"] +  "/" + sess["analog_pulse"])
			new_batch.write("\n")

		if sess["digital_pulse"] != "":

			new_batch.write(sess["session_path"] + "/" + sess["digital_pulse"])
			new_batch.write(" ")
			new_batch.write(biowulf_dest_path + "/" + sess["session_name"] + "/" + sess["digital_pulse"])
			new_batch.write("\n")

	new_batch.close()

	print("\n\n")
	print("biowulf_dest_path: " + biowulf_dest_path)
	print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^  -- MAKE SURE THIS IS RIGHT\n\n")
	print("*** run the bash script created here on biowulf")



# try:
# 	globus_transfer_dir_check = str(subprocess.check_output(["globus", "ls", os.environ["NIH_GLOBUS"] + ":" + nih_dest_subj]))
# #if "Globus CLI Error" in globus_transfer_dir_check:
# except:
# 	print("IGNORE: THIS ERROR HAS BEEN TAKEN INTO ACCOUNT ^")
# 	new_bash.write("mkdir " + nih_dest_subj + "\n")
