import yaml
import yaycl_crypt
import yaycl

conf = yaycl.Config('/home/ashishgusai/assesment2/code_github/', crypt_key='/home/ashishgusai/assesment2/code_github/secret_text')
#yaycl_crypt.encrypt_yaml(conf, 'config')
yaycl_crypt.decrypt_yaml(conf, 'config')
file1=open("/home/ashishgusai/assesment2/code_github/config.yaml")
yaycl_crypt.encrypt_yaml(conf, 'config')
cfg=yaml.load(file1)


