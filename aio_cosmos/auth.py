# The MIT License (MIT)
# Copyright (c) 2014 Microsoft Corporation

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""Authorization helper functions in the Azure Cosmos database service.
"""

import base64
from hashlib import sha256
import hmac
from urllib.parse import quote

from . import http_constants


def get_authorization_header(verb, resource_id_or_fullname, resource_type, headers: dict, master_key: str):

    return __get_authorization_token_using_master_key(
        verb, resource_id_or_fullname, resource_type, headers, master_key
    )


def __get_authorization_token_using_master_key(verb, resource_id_or_fullname, resource_type, headers, master_key):
    """Gets the authorization token using `master_key.

    :param str verb:
    :param str resource_id_or_fullname:
    :param str resource_type:
    :param dict headers:
    :param str master_key:
    :return: The authorization token.
    :rtype: dict

    """

    # decodes the master key which is encoded in base64
    key = base64.b64decode(master_key)

    # Skipping lower casing of resource_id_or_fullname since it may now contain "ID"
    # of the resource as part of the fullname
    text = "{verb}\n{resource_type}\n{resource_id_or_fullname}\n{x_date}\n{http_date}\n".format(
        verb=(verb.lower() or ""),
        resource_type=(resource_type.lower() or ""),
        resource_id_or_fullname=(resource_id_or_fullname or ""),
        x_date=headers.get(http_constants.HttpHeaders.XDate, "").lower(),
        http_date=headers.get(http_constants.HttpHeaders.HttpDate, "").lower(),
    )

    body = text.encode("utf-8")
    digest = hmac.new(key, body, sha256).digest()
    signature = base64.encodebytes(digest).decode("utf-8")

    sig = "type=master&ver=1.0&sig={sig}".format(sig=signature[:-1])
    # -_.!~*'() are valid characters in url, and shouldn't be quoted.
    sig = quote(sig, "-_.!~*'()")
    return sig
