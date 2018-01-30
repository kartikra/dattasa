import subprocess


def send_email_notification(subject, to, body, attach_files='', attach_names=''):

    attach_list = attach_files.split(',')
    attach_names = attach_names.split(',')

    index = 0
    command = "("
    for attach_file in attach_list:

        try:
            attach_name = attach_names[index]
        except:
            attach_name = attach_file

        command += "uuencode " + attach_file + attach_name + ";"
        index += 1

    command = "echo " + '"' + body + '"' + ") | mailx -s  " + '"' + subject + '" ' + to

    print (command)
    subprocess.call(command, shell=True)

    return
