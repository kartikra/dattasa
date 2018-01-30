import paramiko
import sys
import yaml
import os
import gnupg
import pprint
import json
import csv


class File():

    def __init__(self, file_name):
        self.file = open(file_name, 'r')
        return

    def encrypt(self, gpg_key_user, gpg_output_file, log_file, gnupg_home="", verbose=False):

        log_out = open(log_file, 'a')
        gpg_fingerprint = ""
        if gnupg_home == '':
            gnupg_home = os.environ['HOME'] + '/.gnupg'
        gpg = gnupg.GPG(gnupghome=gnupg_home, verbose=verbose)
        gpg_list = gpg.list_keys()
        for gpg_key in gpg_list:
            if verbose:
                pprint.pprint(gpg_key)
            if gpg_key_user in gpg_key['uids']:
                gpg_fingerprint = gpg_key['fingerprint']
                break

        if gpg_fingerprint != "":
            status = gpg.encrypt_file(self.file, gpg_fingerprint, output=gpg_output_file)

            if verbose:
                print ('ok: ' + str(status.ok))
                print ('status: ' + str(status.status))

            if not status.ok:
                '''  run this command manually if needed to troubleshoot '''
                gpg_command = "gpg --encrypt --recipient " + gpg_key_user + " " + self.file.name + \
                              " && mv " + self.file.name + ".gpg " + gpg_output_file
                log_out.write(gpg_command)
                log_out.write("run this command manually if needed to troubleshoot" + "\n")
                log_out.write(gpg_command + "\n")
                log_out.write("Encyrption failed. Status : " + str(status.status) + "\n")
                self.file.close()
                log_out.close()
                raise Exception('Encyrption failed. Status : ' + str(status.status))
        else:
            log_out.write("No valid gpg key found for " + gpg_key_user + " \n")
            self.file.close()
            log_out.close()
            raise Exception('No valid gpg key found for ' + gpg_key_user)

        return open(gpg_output_file, 'r')

    def row_count(self):

        try:
            with self.file as in_file:
                count = sum(1 for _ in in_file)
            self.file.close()
        except:
            print (sys.exc_info())
            self.file.close()
            raise Exception('Unable to obatin row count')

        return count

    def csv_to_fixed_width(self, out_file_name, input_header_row=False, input_delimiter='|',
                           output_field_length=[], output_field_alignment=[]):

        try:
            with self.file as in_file, open(out_file_name, "w") as output_file:
                line_no = 1
                for line in in_file:
                    ''' convert each csv line to fixed width line '''
                    out_line = ''
                    word_list = line.split(input_delimiter)
                    position = 0
                    for word in word_list:
                        ''' ljust or rjust depends on output_field_alignment value of 1 or 0
                        header row is always left justified '''
                        if output_field_alignment[position] == 1 or (line_no == 1 and input_header_row):
                            ''' left justify '''
                            out_line += word.strip().ljust(output_field_length[position])
                        else:
                            ''' right justify '''
                            out_line += word.strip().rjust(output_field_length[position])
                        position += 1
                    output_file.write(out_line + '\n')
                    line_no += 1

                ''' Finally close the file '''
                output_file.close()

        except:
            print (sys.exc_info())
            self.file.close()
            output_file.close()
            raise Exception('Unable to convert to fixed width')

        return

    def replace_words(self, word_replacements={}, inplace=True, output_file_name=''):

        backup_file_name = self.file.name + '.bkup'
        with open(backup_file_name, "w") as output_file, self.file as input_file:
            for line in input_file:
                if sys.version_info.major == 2:
                    for key, value in word_replacements.iteritems():
                        line = line.replace(key, value)
                else:
                    for key, value in word_replacements.items():
                        line = line.replace(key, value)
                output_file.write(line)
            input_file.close()
            output_file.close()

        if inplace:
            os.rename(backup_file_name, self.file.name)
        else:
            os.rename(backup_file_name, output_file_name)

        ''' Open the file again '''
        self.file = open(self.file.name, 'r')

        return


class FileTransfer():

    def __init__(self, sftp_site, config_file=''):
        if config_file == '':
            self.config_file = os.environ['HOME'] + "/ftpsites.yaml"
        else:
            self.config_file = config_file

        try:
            from yaml import CLoader as Loader , CDumper as Dumper
        except ImportError:
            from yaml import Loader, Dumper

        with open(config_file , 'r') as f:
            file_credentials = yaml.load(f, Loader=Loader)
            self.sftp_site = sftp_site
            self.sftp_credentials = file_credentials[sftp_site]

        return

    def sftp_send_file(self, local_file_path, local_file_list, log_file,
                       remote_file_list='', encrypted_password=False):

        sftp_credentials = self.sftp_credentials
        sftp_site = self.sftp_site
        host = sftp_credentials['host']
        port = sftp_credentials['port']
        username = sftp_credentials['user']
        remote_file_path = sftp_credentials['folder']
        if not encrypted_password:
            password = sftp_credentials['password']

        ''' Create list of files to be sent '''
        local_file_list = local_file_list.split(',')
        remote_file_list = remote_file_list.split(',')

        log_out = open(log_file, 'a')
        log_out.write("\n\nstarting sftp to site : " + sftp_site + "\n")
        if remote_file_path is not None:
            log_out.write("sftp to remote_file_path : " + remote_file_path + "\n")
            remote_file_path += "/"
        else:
            log_out.write("sftp to default home directory \n")
            remote_file_path = "/"
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, password=password)

        file_index = 0
        for local_file_name in local_file_list:

            try:
                remote_file_name = remote_file_list[file_index]
            except:
                remote_file_name = local_file_name

            try:
                log_out.write("sending file: " + local_file_name + "\n")

                sftp = paramiko.SFTPClient.from_transport(transport)
                remote_file = remote_file_path + remote_file_name
                local_file = local_file_path + local_file_name
                sftp.put(local_file, remote_file)
                sftp.close()
                transport.close()
                log_out.write("sftp put complete!! \n")
                log_out.close()

            except OSError as err:
                log_out.write("ERROR: OS error - " + format(err) + "\n")
                log_out.close()
                print ("ERROR: OS error - " + format(err) + "\n")
            except:
                log_out.write("ERROR: Unexpected error while sending " + str(sys.exc_info()[0]) + "\n")
                log_out.close()
                print ("ERROR: Unexpected error - ")
                print (sys.exc_info())

            file_index += 1

        return


def _byteify(data, ignore_dicts=False):
    # if this is a unicode string, return its string representation
    if sys.version_info.major == 2:
        if isinstance(data, unicode):
            return data.encode('utf-8')
    # if this is a list of values, return list of byteified values
    if isinstance(data, list):
        return [_byteify(item, ignore_dicts=True) for item in data]
    # if this is a dictionary, return dictionary of byteified keys and values
    # but only if we haven't already byteified it
    if isinstance(data, dict) and not ignore_dicts:
        return {
            _byteify(key, ignore_dicts=True): _byteify(value, ignore_dicts=True)
            for key, value in data.items()
        }
    # if it's anything else, return it in its original form
    return data


def json_load_byteified(file_handle):
    return _byteify(
        json.load(file_handle, object_hook=_byteify) ,
        ignore_dicts=True
    )


def json_loads_byteified(json_text):
    return _byteify(
        json.loads(json_text, object_hook=_byteify),
        ignore_dicts=True
    )


def write_dict_to_csv(csv_file, csv_columns, dict_data, delimiter):
    try:
        with open(csv_file , 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns, delimiter=delimiter)
            writer.writeheader()
            for data in dict_data:
                writer.writerow(data)
    except IOError:
        print("I/O error({0}): {1}".format(IOError.errno, IOError.strerror))
    return
