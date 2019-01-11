import yaml
import yaycl_crypt
import yaycl

conf = yaycl.Config('/Internship_assessments/DonorsChooseCode/', crypt_key='/Internship_assessments/DonorsChooseCode/secret_text')
#yaycl_crypt.encrypt_yaml(conf, 'config')
yaycl_crypt.decrypt_yaml(conf, 'config')
file1=open("/Internship_assessments/DonorsChooseCode/config.yaml")
yaycl_crypt.encrypt_yaml(conf, 'config')
cfg=yaml.load(file1)
