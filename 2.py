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

# input ip, username and password to login
host=input('IP Server you want to connect : ')
port=22
user=input('User to login : ')
passw=getpass.getpass('password : ')

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
        logger.info('work directory: %s' %fileServerPath)

        # download coverAverage.tar.gz
        def downloadCoverAverage():
            try:
                sftp.get(plugin_outServ + 'coverAverage-%s.tar.gz' % filenameTar, \
                         plugin_out + 'coverAverage-%s.tar.gz' % filenameTar)
                logger.info('download coverAverage complete')
            except IOError:
                logger.error('file not exist')
                pass

        # download variant.tar.gz
        def downloadVariant():
            try:
                sftp.get(plugin_outServ + 'variant-%s.tar.gz' % filenameTar, \
                         plugin_out + 'variant-%s.tar.gz' % filenameTar)
                logger.info('download variant complete')
            except IOError:
                logger.error('file not exist')
                pass

        # compress all coverAverage directory
        def tarCoverAverage():
            try:
                command('cd %s ; tar -czf coverAverage-%s.tar.gz --anchored cov*' \
                        % (plugin_outServ, filenameTar))
                time.sleep(0.1)
                logger.info('compress coverAverage complete')
            except IOError:
                logger.error('file not exist')
                pass

        # compress all variant directory
        def tarVariant():
            try:
                command('cd %s ; tar -czf variant-%s.tar.gz --anchored var*' \
                        % (plugin_outServ, filenameTar))
                time.sleep(0.1)
                logger.info('compress variant complete')
            except IOError:
                logger.error('file not exist')
                pass

        # if already folder in client just check file
        # if not exist make directory
        if fileInResultDir.filename in os.listdir(wd1) :  # if folder exist in client
            logger.info('folder : %s already exist' %fileInResultDir.filename)
            for fileInRun in sftp.listdir_attr(fileServerPath):
                # if not exist folder "BAM" in client, make it
                if not ('BAM' in os.listdir(wd2)):
                    os.mkdir(bam)
                    logger.info('create BAM folder complete')
                # if not exist folder "plugin_out" in client, make it
                elif not ('plugin_out' in os.listdir(wd2)):
                    os.mkdir(wd2+'plugin_out')
                    logger.info('create plugin_out folder complete')
                # if report.pdf in client not exist, download
                elif not('report.pdf' in os.listdir(wd2)) and fileInRun.filename == 'report.pdf':
                    sftp.get(fileServerPath+fileInRun.filename,wd2+fileInRun.filename)
                    logger.info('download report.pdf complete')

                # check if exist file in client
                # if file site not equal, download again
                elif 'BAM' in os.listdir(wd2) and 'bam' in fileInRun.filename:
                    # check file *.bam between server and client if size equal,it same file
                    if fileInRun.filename in os.listdir(bam) and 'bam' in fileInRun.filename:
                        if fileInRun.st_size == os.path.getsize(bam+fileInRun.filename):
                            logger.info('%s already exist' % fileInRun.filename)
                        else:
                            sftp.get(fileServerPath+'/'+fileInRun.filename,bam+fileInRun.filename)
                            logger.info('download %s complete' % fileInRun.filename)
                    # if bam not exist in client, download it
                    elif not(fileInRun.filename in os.listdir(bam)) and 'bam' in fileInRun.filename:
                        sftp.get(fileServerPath +'/' + fileInRun.filename, bam + fileInRun.filename)
                        logger.info('download %s complete' %fileInRun.filename)
                    else :
                        pass

                # check file report.pdf
                elif 'report.pdf' in os.listdir(wd2) and 'report' in fileInRun.filename:
                    if fileInRun.st_size == os.path.getsize(wd2+fileInRun.filename):
                        logger.info('%s already exist' % fileInRun.filename)
                    else:
                        sftp.get(fileServerPath+fileInRun.filename,wd2+fileInRun.filename)
                        logger.info('download %s complete' %fileInRun.filename)

                # check folder plugin_out
                elif 'plugin_out' in fileInRun.filename:
                    covertargz = 0  # 0=not exist, 1=exist
                    vartargz = 0
                    fileCover =0
                    fileVariant = 0
                    for fileinPlugin in sftp.listdir_attr(plugin_outServ):
                        if ('cover' in fileinPlugin.filename and '.tar.gz' in fileinPlugin.filename):
                            covertargz += 1
                        elif ('var' in fileinPlugin.filename and '.tar.gz' in fileinPlugin.filename):
                            vartargz += 1
                        elif 'cover' in fileinPlugin.filename and not('.tar.gz' in fileinPlugin.filename):
                            fileCover +=1
                        elif 'variant' in fileinPlugin.filename and not('.tar.gz' in fileinPlugin.filename):
                            fileVariant +=1
                        else:
                            pass

                    # if plugin_out in server is empty, do nothing
                    if fileCover == 0 and fileVariant == 0:
                        logger.error('coverAverage and variant not exist')
                        pass
                    # if exist both directory and not compress, compress and download
                    elif fileCover >= 1 and fileVariant >=1 and covertargz ==0 and vartargz == 0:
                        tarCoverAverage()
                        tarVariant()
                        downloadCoverAverage()
                        downloadVariant()
                    # if exist only variant and compress not yet, compress and download
                    elif fileCover == 0 and fileVariant >= 1 and vartargz == 0:
                        logger.error('coverAverage not exist')
                        tarVariant()
                        downloadVariant()
                    # if coverAverage only exist and compress not yet, compress and download
                    elif fileCover >= 1 and fileVariant == 0 and covertargz == 0:
                        logger.error('variant not exist')
                        tarCoverAverage()
                        downloadCoverAverage()
                    # if both targz exist, download
                    elif covertargz == 1 and vartargz ==1 :
                        logger.info('both targz are exist')
                        downloadCoverAverage()
                        downloadVariant()

                    # check file and download .tar.gz
                    elif 'plugin_out' in fileInRun.filename and 'plugin_out' in os.listdir(wd2) and len(\
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

        # if folder not exist in client
        # make folder, compress file and download
        else:
            logger.info('not found %s folder' %fileInResultDir.filename)
            os.mkdir(wd2)
            logger.info('create folder %s complete' %fileInResultDir.filename)
            os.mkdir(bam)
            logger.info('create folder BAM in %s complete' % fileInResultDir.filename)
            os.mkdir(plugin_out)
            logger.info('create folder plugin_out in %s complete' % fileInResultDir.filename)

            # download file
            for fileInEachRun in sftp.listdir_attr(fileServerPath):
                if '.bam' in fileInEachRun.filename:
                    sftp.get(fileServerPath+fileInEachRun.filename, bam+fileInEachRun.filename)
                    logger.info('download %s complete' %fileInEachRun.filename)
                elif 'report.pdf' in fileInEachRun.filename:
                    sftp.get(fileServerPath+fileInEachRun.filename, wd2+fileInEachRun.filename)
                    logger.info('doenload %s complete' %fileInEachRun.filename)
                elif 'plugin_out' in fileInEachRun.filename:
                    covertargz = 0  # 0=not exist, 1=exist
                    vartargz = 0
                    fileCover = 0
                    fileVariant = 0
                    for fileinPlugin in sftp.listdir_attr(plugin_outServ):
                        if ('cover' in fileinPlugin.filename and '.tar.gz' in fileinPlugin.filename):
                            covertargz += 1
                        elif ('var' in fileinPlugin.filename and '.tar.gz' in fileinPlugin.filename):
                            vartargz += 1
                        elif 'cover' in fileinPlugin.filename and not ('.tar.gz' in fileinPlugin.filename):
                            fileCover += 1
                        elif 'variant' in fileinPlugin.filename and not ('.tar.gz' in fileinPlugin.filename):
                            fileVariant += 1
                        else:
                            pass

                    if fileCover == 0 and fileVariant == 0:
                        logger.error('coverAverage and variant not exist')
                        pass
                    elif fileCover >= 1 and fileVariant >= 1 and covertargz == 0 and vartargz == 0:
                        tarCoverAverage()
                        tarVariant()
                        downloadCoverAverage()
                        downloadVariant()
                    elif fileCover == 0 and fileVariant >= 1 and vartargz == 0:
                        logger.error('coverAverage not exist')
                        tarVariant()
                        downloadVariant()
                    elif fileCover >= 1 and fileVariant == 0 and covertargz == 0:
                        logger.error('variant not exist')
                        tarCoverAverage()
                        downloadCoverAverage()
                    elif covertargz == 1 and vartargz == 1:
                        logger.info('both targz are exist')
                        downloadCoverAverage()
                        downloadVariant()
                    else:
                        pass
                else:
                    pass
    else:
        logger.info('%s is not target' % fileInResultDir.filename)
    logger.info('backup complete')
    logger.info('time use : %s' %(datetime.now()-startTime))

#close both connection
sftp.close()
client.close()

