import paramiko
import os
import stat
from datetime import datetime
import time
import getpass

startTime = datetime.now()
class bcolors:
    OKBLUE = '\033[94m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'

#command
def command(command):
    stdin, stdout, stderr = client.exec_command(command)

# host=input('IP Server you want to connect : ')
# port=22
# user=input('User to login : ')
# passw=getpass.getpass('password : ')

host='192.168.111.112'
port=22
user='mork'
passw='rs43392'
#connect ssh
try:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host,username=user,password=passw)
except paramiko.SSHException:
    print("ssh connection Error")

#connect sftp
try:
    transport = paramiko.Transport((host,port))
    transport.connect(username=user,password=passw)
    sftp = paramiko.SFTPClient.from_transport(transport)
except paramiko.SFTP_FAILURE:
    print("sftp error")

#check file and folder
serverPath = '/home/mork/backupMe'
for fileInResultDir in sftp.listdir_attr(serverPath): #for every file in backup directory
    print("in fisrt for loop folder name = ", fileInResultDir.filename)
    if stat.S_ISDIR(fileInResultDir.st_mode) and 'URRC' in fileInResultDir.filename\
            and not('_tn_' in fileInResultDir.filename): # check file or directory
        wd1 = 'e:/URRC/pycharmProject/backupServer/backupArea/'
        wd2 = 'e:/URRC/pycharmProject/backupServer/backupArea/%s/' % fileInResultDir.filename
        bam = wd2 + 'BAM/'
        fileServerPath = serverPath + "/" + fileInResultDir.filename + "/"
        plugin_out = wd2 + 'plugin_out/'
        plugin_outServ = fileServerPath + 'plugin_out/'
        filenameTar = fileInResultDir.filename[fileInResultDir.filename.index('URRC-') \
                                               :fileInResultDir.filename.index('URRC-') + 7]

        def downloadCoverAverage():
            sftp.get(plugin_outServ + 'coverAverage-%s.tar.gz' % filenameTar, \
                     plugin_out + 'coverAverage-%s.tar.gz' % filenameTar)
            print('download coverAverage complete')

        def downloadVariant():
            sftp.get(plugin_outServ + 'variant-%s.tar.gz' % filenameTar, \
                     plugin_out + 'variant-%s.tar.gz' % filenameTar)
            print('download variant complete')

        def tarCoverAverage():
            print('compressing file... .. .')
            command('cd %s ; tar -czf coverAverage-%s.tar.gz --anchored cov*' \
                    % (plugin_outServ, filenameTar))
            print('time sleep 10 second please wait')
            time.sleep(10)
            print('compress cover complete')

        def tarVariant():
            print('compressing file... .. .')
            command('cd %s ; tar -czf variant-%s.tar.gz --anchored var*' \
                    % (plugin_outServ, filenameTar))
            print('time sleep 10 second please wait')
            time.sleep(10)
            print('compress variant complete')

        if fileInResultDir.filename in os.listdir(wd1) :  #if already folder ่ just check file
            print("[warning] folder : ",fileInResultDir.filename," is already exist")
            for fileInRun in sftp.listdir_attr(fileServerPath):
                if not ('BAM' in os.listdir(wd2)):
                    os.mkdir(bam)
                    print("[+] create BAM folder complete")
                    print("list bam = ",os.listdir(bam))
                elif not ('plugin_out' in os.listdir(wd2)):
                    os.mkdir(wd2+'plugin_out')
                    print('[+] create folder :',fileInRun.filename, ' complete')
                elif not('report.pdf' in os.listdir(wd2)) and fileInRun.filename == 'report.pdf':
                    sftp.get(fileServerPath+fileInRun.filename,wd2+fileInRun.filename)
                    print('[+] download ', fileInRun.filename, ' complete')
                elif 'BAM' in os.listdir(wd2) and 'bam' in fileInRun.filename:
                    if fileInRun.filename in os.listdir(bam) and 'bam' in fileInRun.filename:
                        if fileInRun.st_size == os.path.getsize(bam+fileInRun.filename):
                            print('[+] ',fileInRun.filename,"is already exist and file size is equal")
                        else:
                            sftp.get(fileServerPath+'/'+fileInRun.filename,bam+fileInRun.filename)
                            print("[+] download ", fileInRun, " complete")
                    elif not(fileInRun.filename in os.listdir(bam)) and 'bam' in fileInRun.filename:
                        sftp.get(fileServerPath +'/' + fileInRun.filename, bam + fileInRun.filename)
                        print("[+] download ", fileInRun.filename , " complete")
                    else :
                        pass

                elif 'report.pdf' in os.listdir(wd2) and 'report' in fileInRun.filename:
                    if fileInRun.st_size == os.path.getsize(wd2+fileInRun.filename):
                        print('[+] ', fileInRun.filename, ' is already exist')
                    else:
                        sftp.get(fileServerPath+fileInRun.filename,wd2+fileInRun.filename)
                        print('[+] download ', fileInRun.filename, ' complete')
                elif 'plugin_out' in fileInRun.filename:
                    i = 0
                    covertargz = 0  # 0=not exist, 1=exist
                    vartargz = 0
                    for fileinPlugin in sftp.listdir_attr(plugin_outServ):
                        if ('cover' in fileinPlugin.filename and '.tar.gz' in fileinPlugin.filename) and 'cover' in \
                                sftp.listdir(plugin_outServ)[i] and '.tar.gz' in sftp.listdir(plugin_outServ)[i]:
                            covertargz += 1
                            print('cover = ', covertargz)
                        elif ('var' in fileinPlugin.filename and '.tar.gz' in fileinPlugin.filename) and 'var' in \
                                sftp.listdir(plugin_outServ)[i] and '.tar.gz' in sftp.listdir(plugin_outServ)[i]:
                            vartargz += 1
                            print('tar = ', vartargz)
                        else:
                            pass
                        i += 1

                    if covertargz == 1 and vartargz == 1:
                        print('both tar already exist')
                        downloadVariant()
                        downloadCoverAverage()
                    elif covertargz == 1 and vartargz == 0:
                        print('only cover compressed')
                        tarVariant()
                        downloadVariant()
                        downloadCoverAverage()
                    elif covertargz == 0 and vartargz == 1:
                        print('only vaiant compressed')
                        tarCoverAverage()
                        downloadVariant()
                        downloadCoverAverage()
                    elif covertargz == 0 and vartargz == 0:
                        print('both not compress yet')
                        tarCoverAverage()
                        tarVariant()
                        downloadVariant()
                        downloadCoverAverage()
                    elif 'plugin_out' in fileInRun.filename and 'plugin_out' in os.listdir(wd2) and len(
                        os.listdir(plugin_out)) != 0:
                        for fileinPlugin in sftp.listdir_attr(plugin_outServ):
                            if fileinPlugin.filename in os.listdir(plugin_out):
                                if 'cover' in fileinPlugin.filename and '.tar.gz' in fileinPlugin.filename:
                                    if fileinPlugin.st_size == os.path.getsize(plugin_out + fileinPlugin.filename):
                                        print("cover already exist in plugin_out client")
                                    else:
                                        downloadCoverAverage()
                                elif 'variant' in fileinPlugin.filename and '.tar.gz' in fileinPlugin.filename:
                                    if fileinPlugin.st_size == os.path.getsize(plugin_out + fileinPlugin.filename):
                                        print('variant already exist in plugin_out client')
                                    else:
                                        downloadVariant()
                                else:
                                    pass
                            else:
                                pass
                    else:
                        pass
                else:
                    pass
        else:
            os.mkdir(wd2)
            print('[+] create folder : ', fileInResultDir.filename, 'complete')
            os.mkdir(bam)
            print('[+] create folder : Bam in ', fileInResultDir.filename, 'complete')
            os.mkdir(plugin_out)
            print('[+] create folder : plugin_out in ', fileInResultDir.filename, ' complete')
            for fileInEachRun in sftp.listdir_attr(fileServerPath):
                #print(fileInEachRun.filename)
                if '.bam' in fileInEachRun.filename:
                    sftp.get(fileServerPath+fileInEachRun.filename, bam+fileInEachRun.filename)
                    print("[+] download ",fileInEachRun.filename, " complete")
                elif 'report.pdf' in fileInEachRun.filename:
                    sftp.get(fileServerPath+fileInEachRun.filename, wd2+fileInEachRun.filename)
                    print("[+] download ", fileInEachRun.filename, " complete")
                elif 'plugin_out' in fileInEachRun.filename:
                    i = 0
                    covertargz = 0  # 0=not exist, 1=exist
                    vartargz = 0
                    for fileinPlugin in sftp.listdir_attr(plugin_outServ):
                        if ('cover' in fileinPlugin.filename and '.tar.gz' in fileinPlugin.filename) and 'cover' in \
                                sftp.listdir(plugin_outServ)[i] and '.tar.gz' in sftp.listdir(plugin_outServ)[i]:
                            covertargz += 1
                            print('cover = ', covertargz)
                        elif ('var' in fileinPlugin.filename and '.tar.gz' in fileinPlugin.filename) and 'var' in \
                                sftp.listdir(plugin_outServ)[i] and '.tar.gz' in sftp.listdir(plugin_outServ)[i]:
                            vartargz += 1
                            print('tar = ', vartargz)
                        else:
                            pass
                        i += 1

                    if covertargz == 1 and vartargz == 1:
                        print('both tar already exist')
                        downloadVariant()
                        downloadCoverAverage()
                    elif covertargz == 1 and vartargz == 0:
                        print('only cover compressed')
                        tarVariant()
                        downloadVariant()
                        downloadCoverAverage()
                    elif covertargz == 0 and vartargz == 1:
                        print('only vaiant compressed')
                        tarCoverAverage()
                        downloadVariant()
                        downloadCoverAverage()
                    elif covertargz == 0 and vartargz == 0:
                        print('both not compress yet')
                        tarCoverAverage()
                        tarVariant()
                        downloadVariant()
                        downloadCoverAverage()
                    else:
                        pass

                else:
                    pass
    else:
        print('[-] folder ', fileInResultDir.filename," is not dir or not backup target")

sftp.close()
client.close()
print('start time is : ', startTime)
print('now time is :', datetime.now())
print('###finish### \ntime use to run script : ',datetime.now()-startTime)

