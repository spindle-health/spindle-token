import pytest
import os
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    # For PyArrow 2.0.0 and above, PYARROW_IGNORE_TIMEZONE environment variable must be set
    # to 1 (on the driver and executors).
    os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
    return (
        SparkSession.Builder().appName("spindle-token tests").master("local[2]").getOrCreate()
    )


@pytest.fixture(scope="session")
def private_key() -> bytes:
    """
    WARNING:
        This encryption key is hardcoded so that we can use tests to ensure stability
        of tokens across versions of the spindle-token project.
        It should NEVER be used in production or any environment aside from the test suite
        of the spindle-token project or another OPPRL implementation.
    """
    return b"""-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDln+C4HlLMOLru
7888bbHIf7iG6d+uF7icTLsC7w4UD9dmDJpTwkdfy9LBaDzrSoLmclTfHjN6GIqe
auFDbzsI2OWnFzmpJnudD9W3NcsnNUvoXpOw8iEVbWSIQfPC9T9DIAl2ulY1diMN
p4/Xv4hjvT/MsKqrKtJsXoXsBG5elUnBUP49KZm/YFbeyVYbO1XMM0y1WXMHdmMQ
2u86D4Sk+J3lJh8pavHy26t06VdNnokh5YkylOYlk8dbfaKIsHwUgcGTCQsr5Ij7
YirqathiJw8DyZqKNj5D5gDg+L+NBw/JPQIG/WRmuZb8BDYD0H8vKe1K0T7yB3/n
HLqDFV9XAgMBAAECggEAa80gs1+tEGprvEQCX0TCa7Yme9Qtam6BgYN2Ypy3V8zO
G388LqltV7h5Y8pbmzz6clTnqm3jzeTaox/bTIzaQr+NJWI+dD54Vxa5h08uZmRb
lCZeoPozdPtL8LpBxdvcTNesCbzWPzgModryKWAmmGifHKouHbi3XCAOMG0uIaGB
wSdY5aCaIJ0jSV+gnrmpc6RiaX7PHAhe3ySahMW60MosSeQJdhCX+nkBhhjMQ01a
WdRnVcobcphLIHKNlwrZPnBcvIHrRGhXaamluOfP4GgDRlRhht7r3dwF7P18Ec8l
RQRgHUAQIQ3s0HsmEoq/Avt4LxzZhOGwo/FeozPTpQKBgQD9uywYspRnkfkSbXGa
y1qYewd+oJbfNOe1jcdagvhycOuI5wdYDS1PzbQRw+o4mK668SOQEcmbXd1WekWm
WS0aHvOeOOfXznhzsaDsxzGZd9t8mGt7jYUt8r4rpr1vbbvKukeG3oqjy9a3NhPz
xKAFOrYLqFlD6FQbP0q0zZ2nXQKBgQDnrYWgNhSDgQSJmjWa96l09KHZ582AV2gA
OcX7np8o8nkykCVZF5KQbzk/g2fyT+VrkyMFj7ort9fxA1GR+mqqEO7O2KhimgcP
ytrP/rV+Mum65Ugyo084rFDIrQnLECLxpxfil8U+E6L2JuFuZueK+gcMXAxRG2es
Lmy4TY26QwKBgEkaXlAXU46rjflzMuIIaOe5uqUPG6jfk0rJuIApb+94/UOKXKFP
2pg9if38VkVMKDuQMlDV7yImXxkFTX/p/YajGEmj15hj0mxy5wyRL4cp80DooT8N
vStJ3uSwu6+ZDBwSo30GLSeYGRrcJ895/SjEi6K8NEvOaj0XdSy2eQ+5AoGAUfmG
/5JDHejHxzyGXSfmkNiCcEzh3NiQlwvaLtcIb/+jvK1eWbDKjkqckgmX/RgiEDdG
vBAICrIJOWE4OpPFrBlDtnLY2dKwTvkzBUeVxmqTYyr/qdM5E8984hTAE1bJVoIO
JIIZK4bdMYW+XEcTBIzFc60aOpSc0UK3+1qNnGsCgYEApaaB8053CK8DyHFcNeIp
OLnIFEMaDperCdgWv1S8gGvGnh2bDKyccUW2BTO8BqMB47YkYXUvvH2kOGt5MmxK
dccych5YFsjF+0vDJ1hul5+ZiAA+yFQWUj07/jR+CVKUJMWb5j11jjtmpiTPhmkt
0HVXZ+f7Z8lmid8Fp/q6XVs=
-----END PRIVATE KEY-----
"""


@pytest.fixture(scope="session")
def acme_public_key() -> bytes:
    """
    WARNING:
        This encryption key is hardcoded so that we can use tests to ensure stability
        of tokens across versions of the spindle-token project.
        It should NEVER be used in production or any environment aside from the test suite
        of the spindle-token project or another OPPRL implementation.
    """
    return b"""-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAnIST0H9sM2y0PUyMf5+F
eCKn86xJzSkmjkP0Pe/N5tJfXl7ouRouwSqnOJGIFgEZfnU0pbIsT74hXvKnL8rL
kn9ti5qLLoRIvqG9sfMITUfMjP7z5oZWxrFLo/nQBVRkHMQaFgA/vpj+K79p2fky
JpMqei4SvZAQcVE4t6aBhIa+hra5ctqfIWgr2dSaqZC8G68VEqaF+OwAXJGNzJeI
FBtaQp4TtaR4ZW+qlNfJemr5B3VIxnSPmbKQARPtLnQC2HMYOlBQR1jd8kEV1wAQ
uvz2/aNpCh8fA3FNZ3Q6Y8eaGH++vgrERvNqvkugI1MGq6AEOmMwFleautdNahv2
fQIDAQAB
-----END PUBLIC KEY-----
"""


@pytest.fixture(scope="session")
def acme_private_key() -> bytes:
    """
    WARNING:
        This encryption key is hardcoded so that we can use tests to ensure stability
        of tokens across versions of the spindle-token project.
        It should NEVER be used in production or any environment aside from the test suite
        of the spindle-token project or another OPPRL implementation.
    """
    return b"""-----BEGIN PRIVATE KEY-----
MIIEugIBADANBgkqhkiG9w0BAQEFAASCBKQwggSgAgEAAoIBAQCchJPQf2wzbLQ9
TIx/n4V4IqfzrEnNKSaOQ/Q9783m0l9eXui5Gi7BKqc4kYgWARl+dTSlsixPviFe
8qcvysuSf22LmosuhEi+ob2x8whNR8yM/vPmhlbGsUuj+dAFVGQcxBoWAD++mP4r
v2nZ+TImkyp6LhK9kBBxUTi3poGEhr6Gtrly2p8haCvZ1JqpkLwbrxUSpoX47ABc
kY3Ml4gUG1pCnhO1pHhlb6qU18l6avkHdUjGdI+ZspABE+0udALYcxg6UFBHWN3y
QRXXABC6/Pb9o2kKHx8DcU1ndDpjx5oYf76+CsRG82q+S6AjUwaroAQ6YzAWV5q6
101qG/Z9AgMBAAECgf9jC4J+biQKZr+oCsjd+hR78XJWwK/zJBdB776fcyDY8Edp
cKXUfL0Zf+RR58Y2Jnv7ii/GqvlffPGhI+yoUayBCCJKNYHd7dgayj9AXIrkXIsU
rkGpn1lKYud63JrGxKCVk4LuaTntYuN0y6wwQ4F456f5qS4yxvegvcUmcjeDpMMF
Gir3T7DhPRbPEPEtQ0NxQmCt9nBEKyZeRsdczHtdT27I6tnYYjXY8Gt5/1FrAMKv
eG+jH+GnL5C9XvQzXnDK5keq/CYF3oU7GbuxP1z15BMC+H1IcPy8Rg4YP60c3hFx
uxTAIHhtymYS0WHL44YcemW+GP9ehP1uPhzeTMECgYEA1fSSub+3t7E9vj6QlGbM
yKlCE5+S+eHKbGT0A9QNKJOYnFnucm3y09iRRNqU8qoUi/YLjBKB3tgYbWAmVCHd
JP3hWyH2RUa3ZiFbKSiU9F9218VAvaloTOmx/xELGc8DeQxq6bP8rYUmCCKxXnam
k/TZlrQjOixwWHXydHIdqhkCgYEAu0aA78oVOU8Xg6epjccutkiDadespQM1dqyr
h2GBIzHTp6KaFQNfYdCVSKBRYsyrejPQQtqRecyOunMznOL+Jko6teCf3SmwaW+F
lhhTUcHWqNsUchaNTqkjMU+cDqCMiNnUyswYzTABzkeB1T81bGSZYIigxDAyiwoO
p85BRAUCgYAEDFSaXx5Cw3+/kb96j1dglJNDe8l0IOmLk31qdSCqm+BPVQRF9f6a
M4bMHraiwMUSwN0HEXKTnNMZfrA690kP4h3Zc5MNTIE71XW8GaB33zaIbpsC+6mV
ivZrNQ9YtaoDKFD9iwJE5jgJ/14Lw4LzWbakO1RPTXfytIiZ227kQQKBgEBWeWdn
4qC8plRpnluB6W9gPB15petOzehGa+tnuQjRTmiR+SNs9DDTiuXXhf5OWD70XAEL
u9y/RO2TNFEavruC1uazE9jUQLIISTmGg+FbmZaLJVwImWVDwD5pnjB8QaumQm+l
bIPZkVU/xb/wJjdl3DeI2Mpt0H9p5h0KQ+vNAoGAE2MMAlAMoqM5FWSQYIwi8ij+
oY5DHaeA9U+0G90S59MTn3ueXGNnrBI2Pa6mwa1QaSzcDzk12WzXBo0fbM71pj9A
Fyls61jQVEedIuRk7YtouCaLcZbDyCQJNgxiZpegRkkGwAlJMrhgJ6AT5NS3ChLI
mI7fCXo7x0B5OWnSanc=
-----END PRIVATE KEY-----
"""
