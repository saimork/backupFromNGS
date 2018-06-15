import paramiko
import os
import stat
from datetime import datetime
import time
from tqdm import tqdm
import logging
import getpass


logger = logging.getLogger()
logger.setLevel(logging.INFO)
# create a file handler
handler = logging.FileHandler('backupNGSLog.log')
handler.setLevel(logging.INFO)
# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
# add the handlers to Logger
logger.addHandler(handler)

startTime = datetime.now()

logger.info('start time : %s' %startTime)

#command
def command(command):
    stdin, stdout, stderr = client.exec_command(command)

host=input('IP Server you want to connect : ')
port=22
user=input('User to login : ')
passw=getpass.getpass('password : ')

# host='192.168.111.112'
# port=22
# user='mork'
# passw='rs43392'
#connect ssh
try:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host,username=user,password=passw)
except paramiko.SSHException:
    logger.exception('ssh connection error')
#connect sftp
try:
    transport = paramiko.Transport((host,port))
    transport.connect(username=user,password=passw)
    sftp = paramiko.SFTPClient.from_transport(transport)
except paramiko.SFTP_FAILURE:
    logger.exception('sftp connection error')
#check file and folder
serverPath = '/home/mork/backupMe'
for fileInResultDir in tqdm(sftp.listdir_attr(serverPath),ascii=True,ncols=100): #for every file in backup directory
    if stat.S_ISDIR(fileInResultDir.st_mode) and 'URRC' in fileInResultDir.filename\
            and not('_tn_' in fileInResultDir.filename): #select target directory
        wd1 = 'e:/URRC/pycharmProject/backupServer/backupArea/'
        wd2 = 'e:/URRC/pycharmProject/backupServer/backupArea/%s/' % fileInResultDir.filename
        bam = wd2 + 'BAM/'
        fileServerPath = serverPath + "/" + fileInResultDir.filename + "/"
        plugin_out = wd2 + 'plugin_out/'
        plugin_outServ = fileServerPath + 'plugin_out/'
        filenameTar = fileInResultDir.filename[fileInResultDir.filename.index('URRC-') \
                                               :fileInResultDir.filename.index('URRC-') + 7]

        def downloadCoverAverage(): #download coverAverage.tar.gz
            try:
                sftp.get(plugin_outServ + 'coverAverage-%s.tar.gz' % filenameTar, \
                         plugin_out + 'coverAverage-%s.tar.gz' % filenameTar)
                logger.info('download coverAverage complete')
            except IOError:
                logger.error('file not exist')

        def downloadVariant(): #download variant.tar.gz
            try:
                sftp.get(plugin_outServ + 'variant-%s.tar.gz' % filenameTar, \
                         plugin_out + 'variant-%s.tar.gz' % filenameTar)
                logger.info('download variant complete')
            except IOError:
                logger.error('file not exist')
        def tarCoverAverage(): #compress all coverAverage directory
            try:
                command('cd %s ; tar -czf coverAverage-%s.tar.gz --anchored cov*' \
                        % (plugin_outServ, filenameTar))
                time.sleep(1)
                logger.info('compress coverAverage complete')
            except IOError:
                logger.error('file not exist')
        def tarVariant(): #compress all variant directory
            try:
                command('cd %s ; tar -czf variant-%s.tar.gz --anchored var*' \
                        % (plugin_outServ, filenameTar))
                time.sleep(1)
                logger.info('compress variant complete')
            except IOError:
                logger.error('file not exist')
        if fileInResultDir.filename in os.listdir(wd1) :  #if already folder ่ just check file
            logger.info('folder : %s already exist' %fileInResultDir.filename)
            for fileInRun in sftp.listdir_attr(fileServerPath):
                if not ('BAM' in os.listdir(wd2)):
                    os.mkdir(bam)
                    logger.info('creat BAM folder complete')
                elif not ('plugin_out' in os.listdir(wd2)):
                    os.mkdir(wd2+'plugin_out')
                    logger.info('create plugin_out folder complete')
                elif not('report.pdf' in os.listdir(wd2)) and fileInRun.filename == 'report.pdf':
                    sftp.get(fileServerPath+fileInRun.filename,wd2+fileInRun.filename)
                    logger.info('download report.pdf complete')
                elif 'BAM' in os.listdir(wd2) and 'bam' in fileInRun.filename:
                    if fileInRun.filename in os.listdir(bam) and 'bam' in fileInRun.filename:
                        if fileInRun.st_size == os.path.getsize(bam+fileInRun.filename):
                            logger.info('%s already exist' % fileInRun.filename)
                        else:
                            sftp.get(fileServerPath+'/'+fileInRun.filename,bam+fileInRun.filename)
                            logger.info('download %s complete' % fileInRun.filename)
                    elif not(fileInRun.filename in os.listdir(bam)) and 'bam' in fileInRun.filename:
                        sftp.get(fileServerPath +'/' + fileInRun.filename, bam + fileInRun.filename)
                        logger.info('download %s complete' %fileInRun.filename)
                    else :
                        pass
                elif 'report.pdf' in os.listdir(wd2) and 'report' in fileInRun.filename:
                    if fileInRun.st_size == os.path.getsize(wd2+fileInRun.filename):
                        logger.info('%s already exist' % fileInRun.filename)
                    else:
                        sftp.get(fileServerPath+fileInRun.filename,wd2+fileInRun.filename)
                        logger.info('download %s complete' %fileInRun.filename)
                elif 'plugin_out' in fileInRun.filename:
                    i = 0
                    covertargz = 0  # 0=not exist, 1=exist
                    vartargz = 0

                    for fileinPlugin in sftp.listdir_attr(plugin_outServ):
                        if ('cover' in fileinPlugin.filename and '.tar.gz' in fileinPlugin.filename) and 'cover' in \
                                sftp.listdir(plugin_outServ)[i] and '.tar.gz' in sftp.listdir(plugin_outServ)[i]:
                            covertargz += 1
                        elif ('var' in fileinPlugin.filename and '.tar.gz' in fileinPlugin.filename) and 'var' in \
                                sftp.listdir(plugin_outServ)[i] and '.tar.gz' in sftp.listdir(plugin_outServ)[i]:
                            vartargz += 1
                        else:
                            pass
                        i += 1
                    if covertargz == 1 and vartargz == 1:
                        logger.info('both .tar.gz already exist')
                        downloadVariant()
                        downloadCoverAverage()
                    elif covertargz == 1 and vartargz == 0:
                        logger.info('only coverAverage compressed')
                        tarVariant()
                        downloadVariant()
                        downloadCoverAverage()
                    elif covertargz == 0 and vartargz == 1:
                        logger.info('only variant compressed')
                        tarCoverAverage()
                        downloadVariant()
                        downloadCoverAverage()
                    elif covertargz == 0 and vartargz == 0:
                        logger.info('both not compress yet')
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
                                        logger.info('coverAverage already exist in plugin_out folder')
                                    else:
                                        downloadCoverAverage()
                                elif 'variant' in fileinPlugin.filename and '.tar.gz' in fileinPlugin.filename:
                                    if fileinPlugin.st_size == os.path.getsize(plugin_out + fileinPlugin.filename):
                                        logger.info('variant already exist in plugin_out folder')
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
            logger.info('not found %s folder' %fileInResultDir.filename)
            os.mkdir(wd2)
            logger.info('create folder %s complete' %fileInResultDir.filename)
            os.mkdir(bam)
            logger.info('create folder BAM in %s complete' % fileInResultDir.filename)
            os.mkdir(plugin_out)
            logger.info('create folder plugin_out in %s complete' % fileInResultDir.filename)
            for fileInEachRun in sftp.listdir_attr(fileServerPath):
                if '.bam' in fileInEachRun.filename:
                    sftp.get(fileServerPath+fileInEachRun.filename, bam+fileInEachRun.filename)
                    logger.info('download %s complete' %fileInEachRun.filename)
                elif 'report.pdf' in fileInEachRun.filename:
                    sftp.get(fileServerPath+fileInEachRun.filename, wd2+fileInEachRun.filename)
                    logger.info('doenload %s complete' %fileInEachRun.filename)
                elif 'plugin_out' in fileInEachRun.filename:
                    i = 0
                    covertargz = 0  # 0=not exist, 1=exist
                    vartargz = 0
                    for fileinPlugin in sftp.listdir_attr(plugin_outServ):
                        if ('cover' in fileinPlugin.filename and '.tar.gz' in fileinPlugin.filename) and 'cover' in \
                                sftp.listdir(plugin_outServ)[i] and '.tar.gz' in sftp.listdir(plugin_outServ)[i]:
                            covertargz += 1
                        elif ('var' in fileinPlugin.filename and '.tar.gz' in fileinPlugin.filename) and 'var' in \
                                sftp.listdir(plugin_outServ)[i] and '.tar.gz' in sftp.listdir(plugin_outServ)[i]:
                            vartargz += 1
                        else:
                            pass
                        i += 1

                    if covertargz == 1 and vartargz == 1:
                        logger.info('both .tar.gz already exist')
                        downloadVariant()
                        downloadCoverAverage()
                    elif covertargz == 1 and vartargz == 0:
                        logger.info('only coverAverage compressed')
                        tarVariant()
                        downloadVariant()
                        downloadCoverAverage()
                    elif covertargz == 0 and vartargz == 1:
                        logger.info('only variant compressed')
                        tarCoverAverage()
                        downloadVariant()
                        downloadCoverAverage()
                    elif covertargz == 0 and vartargz == 0:
                        logger.info('both not compress yet')
                        tarCoverAverage()
                        tarVariant()
                        downloadVariant()
                        downloadCoverAverage()
                    else:
                        pass

                else:
                    pass
    else:
        logger.info('%s is not target' % fileInResultDir.filename)
    time.sleep(0.1)
    logger.info('backup complete')
    logger.info('time use : %s' %(datetime.now()-startTime))
sftp.close()
client.close()

