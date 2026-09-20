import asyncio
import aiohttp
import json
import re
import random
import os
import logging
from urllib.parse import urlparse
from typing import Tuple, Dict, Any, Optional, Union

from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
import uvicorn

# Configure structured production logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] [ReqID:%(process)d] %(message)s'
)
logger = logging.getLogger("shopify_checker")

# PASTE YOUR GRAPHQL QUERIES HERE
QUERY_PROPOSAL_SHIPPING = """""" 
QUERY_PROPOSAL_DELIVERY = """"""
MUTATION_SUBMIT = """"""
QUERY_POLL = """"""

C2C = {
    "USD": "US", "CAD": "CA", "INR": "IN", "AED": "AE",
    "HKD": "HK", "GBP": "GB", "CHF": "CH",
}

book = {
    "US": {"address1": "123 Main", "city": "NY", "postalCode": "10080", "zoneCode": "NY", "countryCode": "US", "phone": "2194157586"},
    "CA": {"address1": "88 Queen", "city": "Toronto", "postalCode": "M5J2J3", "zoneCode": "ON", "countryCode": "CA", "phone": "4165550198"},
    "GB": {"address1": "221B Baker Street", "city": "London", "postalCode": "NW1 6XE", "zoneCode": "LND", "countryCode": "GB", "phone": "2079460123"},
    "IN": {"address1": "221B MG", "city": "Mumbai", "postalCode": "400001", "zoneCode": "MH", "countryCode": "IN", "phone": "+91 9876543210"},
    "AE": {"address1": "Burj Tower", "city": "Dubai", "postalCode": "", "zoneCode": "DU", "countryCode": "AE", "phone": "+971 50 123 4567"},
    "HK": {"address1": "Nathan 88", "city": "Kowloon", "postalCode": "", "zoneCode": "KL", "countryCode": "HK", "phone": "+852 5555 5555"},
    "CN": {"address1": "8 Zhongguancun Street", "city": "Beijing", "postalCode": "100080", "zoneCode": "BJ", "countryCode": "CN", "phone": "1062512345"},
    "CH": {"address1": "Gotthardstrasse 17", "city": "Schweiz", "postalCode": "6430", "zoneCode": "SZ", "countryCode": "CH", "phone": "445512345"},
    "AU": {"address1": "1 Martin Place", "city": "Sydney", "postalCode": "2000", "zoneCode": "NSW", "countryCode": "AU", "phone": "291234567"},
    "DEFAULT": {"address1": "123 Main", "city": "New York", "postalCode": "10080", "zoneCode": "NY", "countryCode": "US", "phone": "2194157586"},
}

# Pre-compile Regex for maximum parsing efficiency
CLEAN_PATTERNS = [
    re.compile(r'(PAYMENTS_[A-Z_]+)', re.IGNORECASE),
    re.compile(r'(CARD_[A-Z_]+)', re.IGNORECASE),
    re.compile(r'([A-Z]+_[A-Z]+_[A-Z_]+)', re.IGNORECASE),
    re.compile(r'([A-Z]+_[A-Z_]+)', re.IGNORECASE),
    re.compile(r'code["\']?\s*[:=]\s*["\']?([^"\',]+)["\']?', re.IGNORECASE),
    re.compile(r'{"code":"([^"]+)"', re.IGNORECASE),
    re.compile(r"'code':'([^']+)'", re.IGNORECASE)
]

# --- GLOBAL CACHE & LOCKS ---
_VARIANT_CACHE: Dict[str, dict] = {}
_DOMAIN_LOCKS: Dict[str, asyncio.Lock] = {}

class HTTPClient:
    """Global connection pool manager. SSL disabled to allow proxy support."""
    connector: Optional[aiohttp.TCPConnector] = None
    
    @classmethod
    def get_connector(cls) -> aiohttp.TCPConnector:
        if cls.connector is None or cls.connector.closed:
            cls.connector = aiohttp.TCPConnector(
                limit=1000,
                ttl_dns_cache=300,
                ssl=False,
                enable_cleanup_closed=True
            )
        return cls.connector

def pick_addr(url, cc=None, rc=None):
    cc = (cc or "").upper()
    rc = (rc or "").upper()
    dom = urlparse(url).netloc
    tcn = dom.split('.')[-1].upper()

    if tcn in book: return book[tcn]
    ccn = C2C.get(cc)
    if rc in book and ccn == rc: return book[rc]
    elif rc in book: return book[rc]
    return book["DEFAULT"]

def extract_between(text, start, end):
    if not text or not start or not end: return None
    try:
        if start in text:
            parts = text.split(start, 1)
            if len(parts) > 1 and end in parts[1]:
                result = parts[1].split(end, 1)[0]
                return result if result else None
    except Exception: pass
    return None

class Utils:
    @staticmethod
    def get_random_name():
        first_names = ["James", "John", "Robert", "Michael", "William", "David", "Mary", "Patricia", "Jennifer", "Linda"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez"]
        return (random.choice(first_names), random.choice(last_names))
    
    @staticmethod
    def generate_email(first, last):
        domains = ["gmail.com", "yahoo.com", "outlook.com", "protonmail.com"]
        return f"{first.lower()}.{last.lower()}@{random.choice(domains)}"

def parse_proxy(proxy_str):
    if not proxy_str: return None
    parts = proxy_str.split(':')
    if len(parts) == 2: return f"http://{parts[0]}:{parts[1]}"
    elif len(parts) == 4: return f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"
    return None

def is_captcha_required(response_text):
    if not response_text: return False
    indicators = ['CAPTCHA_REQUIRED', '"code":"CAPTCHA_REQUIRED"', "'code':'CAPTCHA_REQUIRED'",
                  '"message":"CAPTCHA_REQUIRED"', 'captcha required', 'CAPTCHA CHALLENGE',
                  'hcaptcha', 'h-captcha']
    text_upper = response_text.upper()
    return any(ind.upper() in text_upper for ind in indicators)

def extract_clean_response(message):
    """Optimized string extraction using regex."""
    if not message: return "UNKNOWN_ERROR"
    message = str(message)
    
    for pattern in CLEAN_PATTERNS:
        for match in pattern.findall(message):
            if isinstance(match, tuple): match = match[0]
            if match and "_" in match and len(match) < 50:
                return match.strip("{}:'\" ")
    
    words = message.split()
    if words and "_" in words[0] and words[0].isupper(): return words[0]
    return message[:50]

async def fetch_products(domain: str, proxy_str: Optional[str], session: aiohttp.ClientSession) -> Union[dict, Tuple[bool, str]]:
    """Fetch products and find the absolute cheapest product under $10 matching sp.py logic."""
    domain = domain if domain.startswith('http') else f"https://{domain}"
    
    if domain in _VARIANT_CACHE and _VARIANT_CACHE[domain]:
        return _VARIANT_CACHE[domain]
        
    if domain not in _DOMAIN_LOCKS:
        _DOMAIN_LOCKS[domain] = asyncio.Lock()
        
    async with _DOMAIN_LOCKS[domain]:
        if domain in _VARIANT_CACHE and _VARIANT_CACHE[domain]:
            return _VARIANT_CACHE[domain]
            
        proxy = parse_proxy(proxy_str)
        try:
            async with session.get(f"{domain}/products.json?limit=250", proxy=proxy, timeout=10) as resp:
                if resp.status != 200: 
                    return False, f"Site Error! Status: {resp.status}"
                text = await resp.text()
                if "shopify" not in text.lower(): 
                    return False, "Not Shopify!"
                
                data = await resp.json()
                products = data.get('products', [])
                if not products: 
                    return False, "No Products!"

            min_price = float('inf')
            min_product = None
            cheap_products = []

            for product in products:
                for variant in product.get('variants', []):
                    if not variant.get('available', True): 
                        continue
                    
                    try:
                        price_raw = variant.get('price', '0')
                        if isinstance(price_raw, str):
                            price = float(price_raw.replace(',', ''))
                        else:
                            price = float(price_raw)
                            
                        if price < 10.00:
                            cheap_products.append({
                                'site': domain,
                                'price': f"{price:.2f}",
                                'variant_id': str(variant['id']),
                                'link': f"{domain}/products/{product.get('handle', '')}"
                            })
                            
                            if price < min_price:
                                min_price = price
                                min_product = {
                                    'site': domain,
                                    'price': f"{price:.2f}",
                                    'variant_id': str(variant['id']),
                                    'link': f"{domain}/products/{product.get('handle', '')}"
                                }
                    except (ValueError, TypeError, AttributeError): 
                        continue
            
            if min_product and min_price < 10.00:
                _VARIANT_CACHE[domain] = min_product
                if len(_VARIANT_CACHE) > 500:
                    _VARIANT_CACHE.clear()
                    _DOMAIN_LOCKS.clear()
                return min_product
            elif cheap_products:
                # Fallback matching sp.py logic if min_product logic somehow misses
                return cheap_products[0]
            else:
                return False, "No products under $10 found!"
                
        except aiohttp.ClientError as e:
            return False, f"Proxy Error: {str(e)}"
        except Exception as e:
            return False, f"error: {str(e)}"

async def make_graphql_request_with_captcha_handling(session, url, params, headers, json_data, proxy, max_retries=1):
    """Execution matched to sp.py."""
    for attempt in range(max_retries + 1):
        try:
            resp = await session.post(url, params=params, headers=headers, json=json_data, proxy=proxy, timeout=15)
            text = await resp.text()
            return resp, text
        except Exception as e:
            if attempt == max_retries: return None, str(e)
            await asyncio.sleep(1)
    return None, "Max retries exceeded"

class CheckoutSession:
    """Encapsulates checkout state per request to prevent variable leaking/race conditions."""
    def __init__(self, cc_info, site_url, explicit_variant_id, proxy_str):
        self.cc = cc_info['cc']
        self.mes = cc_info['mes']
        self.ano = cc_info['ano']
        self.cvv = cc_info['cvv']
        self.site_url = site_url if site_url.startswith('http') else f'https://{site_url}'
        self.domain = urlparse(self.site_url).netloc
        self.explicit_variant_id = explicit_variant_id
        self.proxy_str = proxy_str
        self.proxy = parse_proxy(proxy_str)
        
        addr = pick_addr(self.site_url)
        self.firstName, self.lastName = Utils.get_random_name()
        self.email = Utils.generate_email(self.firstName, self.lastName)
        self.phone, self.street, self.city = addr["phone"], addr["address1"], addr["city"]
        self.state, self.s_zip, self.country = addr["zoneCode"], addr["postalCode"], addr["countryCode"]
        
        # Exact headers from sp.py
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36 Edg/146.0.0.0',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Content-Type': 'application/json',
            'Origin': self.site_url,
            'Referer': self.site_url,
            'sec-ch-ua': '"Chromium";v="146", "Not-A.Brand";v="24", "Microsoft Edge";v="146"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        }
        
        self.gateway = "UNKNOWN"
        self.total_price = "0.00"
        self.currency = "USD"
        
        self.variant_id = None
        self.checkout_url = ""
        self.attempt_token = ""
        self.sst = ""
        self.queueToken = ""
        self.stableId = "1"
        self.merch = ""
        self.build_id = None
        self.source_token = None
        self.ident_sig = None
        
        self.checkpoint_data = None
        self.shipping_amount = 0.0
        self.tax_amount = 0.0
        self.running_total = "0.00"
        self.delivery_strategy = ""
        self.payment_identifier = None
        
    async def _add_to_cart_robust(self, session: aiohttp.ClientSession):
        cart_url = f"{self.site_url}/cart/add.js"
        c_headers = {
            **self.headers,
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json, text/javascript'
        }
        
        if self.explicit_variant_id:
            v_id = self.explicit_variant_id
            self.total_price = "0.00" 
        else:
            info = await fetch_products(self.site_url, self.proxy_str, session)
            if isinstance(info, tuple) and info[0] is False:
                return False, info[1]
            
            v_id = info['variant_id']
            self.total_price = info['price']

        try:
            c_resp = await session.post(cart_url, data=f'id={v_id}&quantity=1', headers=c_headers, proxy=self.proxy)
            
            # Match sp.py fallback logic
            if c_resp.status != 200:
                c_headers_json = {
                    **self.headers,
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                }
                c_resp = await session.post(cart_url, json={'items': [{'id': int(v_id), 'quantity': 1}]}, headers=c_headers_json, proxy=self.proxy)
                
            if c_resp.status == 200:
                self.variant_id = v_id
                return True, "OK"
                    
        except (aiohttp.ClientError, asyncio.TimeoutError):
            return False, "Network error during cart addition"
        
        # If we failed to add to cart, invalidate cache
        if not self.explicit_variant_id and self.domain in _VARIANT_CACHE:
            del _VARIANT_CACHE[self.domain]

        return False, f"Cart failed with status {c_resp.status}"

    async def run(self, session: aiohttp.ClientSession):
        cart_ok, cart_msg = await self._add_to_cart_robust(session)
        if not cart_ok:
            return False, cart_msg, self.gateway, self.total_price, self.currency

        chk_headers = {
            **self.headers,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1'
        }
        
        resp = await session.post(f"{self.site_url}/checkout/", allow_redirects=True, headers=chk_headers, proxy=self.proxy)
        self.checkout_url = str(resp.url)
        
        if 'login' in self.checkout_url.lower():
            return False, "Site requires login!", self.gateway, self.total_price, self.currency

        t_match = re.search(r'/checkouts/cn/([^/?]+)', self.checkout_url)
        self.attempt_token = t_match.group(1) if t_match else self.checkout_url.split('/')[-1].split('?')[0]
        
        text = await resp.text()
        
        # Exact extraction cascade from sp.py
        self.sst = resp.headers.get('X-Checkout-One-Session-Token') or resp.headers.get('x-checkout-one-session-token')
        if not self.sst:
            self.sst = extract_between(text, 'name="serialized-sessionToken" content="&quot;', '&quot;')
            if not self.sst: self.sst = extract_between(text, 'name="serialized-sessionToken" content="', '"')
            if not self.sst: self.sst = extract_between(text, '"serializedSessionToken":"', '"')
            if not self.sst: self.sst = extract_between(text, 'data-session-token="', '"')
            if not self.sst: self.sst = extract_between(text, '"sessionToken":"', '"')
                   
        if not self.sst: return False, "Failed to get session token", self.gateway, self.total_price, self.currency

        self.queueToken = extract_between(text, 'queueToken&quot;:&quot;', '&quot;') or extract_between(text, '"queueToken":"', '"') or ""
        self.stableId = extract_between(text, 'stableId&quot;:&quot;', '&quot;') or extract_between(text, '"stableId":"', '"') or "1"
        self.merch = extract_between(text, 'ProductVariantMerchandise/', '&quot;') or \
                     extract_between(text, 'ProductVariantMerchandise/', '&q') or \
                     extract_between(text, '"merchandiseId":"gid://shopify/ProductVariantMerchandise/', '"') or str(self.variant_id)
        
        self.currency = 'USD'
        if 'currencyCode&quot;:&quot;' in text:
            self.currency = extract_between(text, 'currencyCode&quot;:&quot;', '&quot;') or 'USD'
        elif '"currencyCode":"' in text:
            self.currency = extract_between(text, '"currencyCode":"', '"') or 'USD'
            
        subtotal = extract_between(text, 'subtotalBeforeTaxesAndShipping&quot;:{&quot;value&quot;:{&quot;amount&quot;:&quot;', '&quot;') or \
                   extract_between(text, '"subtotalBeforeTaxesAndShipping":{"value":{"amount":"', '"')
        if not subtotal:
            price_match = re.search(r'"price":\s*"([\d.]+)"', text)
            subtotal = price_match.group(1) if price_match else "0.01"

        unescaped = text.replace('&quot;', '"').replace('&amp;', '&').replace('&#39;', "'")
        bm = re.search(r'"commitSha"\s*:\s*"([a-f0-9]{40})"', unescaped)
        self.build_id = bm.group(1) if bm else None
        
        source_token = extract_between(text, 'name="serialized-sourceToken" content="', '"')
        if source_token: self.source_token = source_token.replace('&quot;', '').strip('"')
        
        im = re.search(r'checkoutCardsinkCallerIdentificationSignature":"([^"]+)"', unescaped)
        if im: self.ident_sig = im.group(1)

        self.headers.update({
            'shopify-checkout-client': 'checkout-web/1.0',
            'shopify-checkout-source': f'id="{self.attempt_token}", type="cn"',
            'x-checkout-one-session-token': self.sst,
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
        })
        if self.build_id:
            self.headers['x-checkout-web-build-id'] = self.build_id
            self.headers['x-checkout-web-deploy-stage'] = 'production'
            self.headers['x-checkout-web-server-handling'] = 'fast'
            self.headers['x-checkout-web-server-rendering'] = 'yes'
        if self.source_token:
            self.headers['x-checkout-web-source-id'] = self.source_token

        graphql_url = f'https://{self.domain}/checkouts/unstable/graphql'

        # Execute Sequential Logic to Match sp.py state manipulation precisely
        neg_ok, neg_msg = await self._execute_negotiations(session, graphql_url, subtotal)
        if not neg_ok: return False, neg_msg, self.gateway, self.total_price, self.currency
        
        tok_ok, token_or_err = await self._tokenize_card(session)
        if not tok_ok: return False, token_or_err, self.gateway, self.total_price, self.currency

        s_ok, s_status, rid = await self._submit_payment(session, graphql_url, token_or_err, subtotal)
        if not s_ok: return False, s_status, self.gateway, self.total_price, self.currency
        if s_status != "POLL": return True, s_status, self.gateway, self.total_price, self.currency

        return await self._poll_receipt(session, graphql_url, rid)

    async def _execute_negotiations(self, session, graphql_url, subtotal):
        params = {'operationName': 'Proposal'}
        
        base_json = {
            'query': QUERY_PROPOSAL_SHIPPING,
            'variables': {
                'sessionInput': {'sessionToken': self.sst},
                'queueToken': self.queueToken,
                'discounts': {'lines': [], 'acceptUnexpectedDiscounts': True},
                'delivery': {
                    'deliveryLines': [{
                        'destination': {
                            'partialStreetAddress': {
                                'address1': self.street, 'address2': '', 'city': self.city,
                                'countryCode': self.country, 'postalCode': self.s_zip,
                                'firstName': self.firstName, 'lastName': self.lastName,
                                'zoneCode': self.state, 'phone': self.phone
                            }
                        },
                        'selectedDeliveryStrategy': {
                            'deliveryStrategyMatchingConditions': {
                                'estimatedTimeInTransit': {'any': True},
                                'shipments': {'any': True}
                            },
                            'options': {}
                        },
                        'targetMerchandiseLines': {'any': True},
                        'deliveryMethodTypes': ['SHIPPING'],
                        'expectedTotalPrice': {'any': True},
                        'destinationChanged': True
                    }],
                    'noDeliveryRequired': [],
                    'useProgressiveRates': False,
                    'prefetchShippingRatesStrategy': None,
                    'supportsSplitShipping': True
                },
                'deliveryExpectations': {'deliveryExpectationLines': []},
                'merchandise': {
                    'merchandiseLines': [{
                        'stableId': self.stableId,
                        'merchandise': {
                            'productVariantReference': {
                                'id': f'gid://shopify/ProductVariantMerchandise/{self.merch}',
                                'variantId': f'gid://shopify/ProductVariant/{self.variant_id}',
                                'properties': [],
                                'sellingPlanId': None,
                                'sellingPlanDigest': None
                            }
                        },
                        'quantity': {'items': {'value': 1}},
                        'expectedTotalPrice': {'value': {'amount': subtotal, 'currencyCode': self.currency}},
                        'lineComponentsSource': None,
                        'lineComponents': []
                    }]
                },
                'payment': {
                    'totalAmount': {'any': True},
                    'paymentLines': [],
                    'billingAddress': {
                        'streetAddress': {
                            'address1': '', 'city': '', 'countryCode': self.country,
                            'lastName': '', 'zoneCode': 'ENG', 'phone': ''
                        }
                    }
                },
                'buyerIdentity': {
                    'customer': {'presentmentCurrency': self.currency, 'countryCode': self.country},
                    'email': self.email,
                    'emailChanged': False,
                    'phoneCountryCode': self.country,
                    'marketingConsent': [{'email': {'value': self.email}}],
                    'shopPayOptInPhone': {'countryCode': self.country},
                    'rememberMe': False
                },
                'tip': {'tipLines': []},
                'taxes': {
                    'proposedAllocations': None,
                    'proposedTotalAmount': {'value': {'amount': '0', 'currencyCode': self.currency}},
                    'proposedTotalIncludedAmount': None,
                    'proposedMixedStateTotalAmount': None,
                    'proposedExemptions': []
                },
                'note': {'message': None, 'customAttributes': []},
                'localizationExtension': {'fields': []},
                'nonNegotiableTerms': None,
                'scriptFingerprint': {
                    'signature': None,
                    'signatureUuid': None,
                    'lineItemScriptChanges': [],
                    'paymentScriptChanges': [],
                    'shippingScriptChanges': []
                },
                'optionalDuties': {'buyerRefusesDuties': False}
            },
            'operationName': 'Proposal'
        }

        # Step 1: Shipping Proposal
        for i in range(2):
            resp, text = await make_graphql_request_with_captcha_handling(
                session, graphql_url, params, self.headers, base_json, self.proxy
            )
            if i == 0: await asyncio.sleep(3)
            
        if not resp: return False, f"Request failed: {text}"
        if is_captcha_required(text): return False, "CAPTCHA_REQUIRED"

        try:
            resp_json = json.loads(text)
        except json.JSONDecodeError as e:
            return False, f"Invalid JSON response: {str(e)}"

        if 'errors' in resp_json:
            error_msgs = [e.get('message', str(e)) for e in resp_json.get('errors', [])[:3]]
            return False, f"GraphQL Error: {'; '.join(error_msgs)}"

        try:
            result = resp_json.get('data', {}).get('session', {}).get('negotiate', {}).get('result', {})
            if not result: return False, "Empty negotiation result"
            
            result_type = result.get('__typename', 'Unknown')
            if result_type in ('CheckpointDenied', 'Throttled', 'NegotiationResultFailed'):
                return False, result_type
            
            self.checkpoint_data = result.get('checkpointData')
            seller_proposal = result.get('sellerProposal', {})
            
            running_total_data = seller_proposal.get('runningTotal')
            if running_total_data:
                self.running_total = running_total_data['value']['amount']

            delivery_data = seller_proposal.get('delivery', {})
            if delivery_data.get('__typename') == 'FilledDeliveryTerms':
                delivery_lines = delivery_data.get('deliveryLines', [{}])
                if delivery_lines:
                    available_strategies = delivery_lines[0].get('availableDeliveryStrategies', [])
                    if available_strategies:
                        self.delivery_strategy = available_strategies[0].get('handle', '')
                        try:
                            self.shipping_amount = float(available_strategies[0].get('amount', {}).get('value', {}).get('amount', '0'))
                        except: self.shipping_amount = 0.0

            tax_data = seller_proposal.get('tax', {})
            if tax_data.get('__typename') == 'FilledTaxTerms':
                try:
                    self.tax_amount = float(tax_data.get('totalTaxAmount', {}).get('value', {}).get('amount', '0'))
                except: self.tax_amount = 0.0

            payment_data = seller_proposal.get('payment', {})
            if payment_data.get('__typename') == 'FilledPaymentTerms':
                for method in payment_data.get('availablePaymentLines', []):
                    pm = method.get('paymentMethod', {})
                    if pm.get('name') or pm.get('paymentMethodIdentifier'):
                        self.payment_identifier = pm.get('paymentMethodIdentifier')
                        self.gateway = pm.get('extensibilityDisplayName') or pm.get('name', 'UNKNOWN')
                        self.total_price = str(float(self.running_total) + self.shipping_amount + self.tax_amount)
                        break

            if not self.payment_identifier: return False, "No valid payment method found"

        except Exception as e:
            return False, f"Failed to parse proposal response: {str(e)}"

        # Step 2: Delivery Proposal
        base_json['query'] = QUERY_PROPOSAL_DELIVERY
        dl = base_json['variables']['delivery']['deliveryLines'][0]
        dl['selectedDeliveryStrategy'] = {
            'deliveryStrategyByHandle': {'handle': self.delivery_strategy, 'customDeliveryRate': False},
            'options': {}
        }
        dl['targetMerchandiseLines'] = {'lines': [{'stableId': self.stableId}]}
        dl['expectedTotalPrice'] = {'value': {'amount': str(self.shipping_amount), 'currencyCode': self.currency}}
        dl['destinationChanged'] = False
        
        base_json['variables']['payment']['billingAddress'] = {
            'streetAddress': {
                'address1': self.street, 'address2': '', 'city': self.city,
                'countryCode': self.country, 'postalCode': self.s_zip,
                'firstName': self.firstName, 'lastName': self.lastName,
                'zoneCode': self.state, 'phone': self.phone
            }
        }
        base_json['variables']['taxes']['proposedTotalAmount']['value']['amount'] = str(self.tax_amount)
        base_json['variables']['buyerIdentity']['shopPayOptInPhone']['number'] = self.phone

        resp, text = await make_graphql_request_with_captcha_handling(
            session, graphql_url, params, self.headers, base_json, self.proxy
        )
        if is_captcha_required(text): return False, "CAPTCHA_REQUIRED on delivery proposal"

        return True, "OK"

    async def _tokenize_card(self, session):
        payload = {
            "credit_card": {
                "number": self.cc,
                "month": int(self.mes),
                "year": int(self.ano),
                "verification_value": self.cvv,
                "start_month": None,
                "start_year": None,
                "issue_number": "",
                "name": f"{self.firstName} {self.lastName}"
            },
            "payment_session_scope": urlparse(self.site_url).netloc
        }
        
        vault_headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
            'Origin': 'https://checkout.pci.shopifyinc.com',
            'Referer': 'https://checkout.pci.shopifyinc.com/build/a8e4a94/number-ltr.html?identifier=&locationURL=',
            'User-Agent': self.headers['User-Agent'],
            'sec-ch-ua': self.headers['sec-ch-ua'],
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-storage-access': 'active',
        }
        if self.ident_sig: vault_headers['shopify-identification-signature'] = self.ident_sig
        
        try:
            resp = await session.post('https://checkout.pci.shopifyinc.com/sessions', json=payload, headers=vault_headers, proxy=self.proxy)
            t_data = await resp.json()
            token = t_data.get('id')
            if not token: return False, 'Unable to get payment token'
            return True, token
        except Exception as e:
            return False, f'Unable to get payment token: {str(e)}'

    async def _submit_payment(self, session, graphql_url, token, subtotal):
        sub_vars = {
            'input': {
                'sessionInput': {'sessionToken': self.sst},
                'queueToken': self.queueToken,
                'discounts': {'lines': [], 'acceptUnexpectedDiscounts': True},
                'delivery': {
                    'deliveryLines': [{
                        'destination': {
                            'streetAddress': {
                                'address1': self.street, 'address2': '', 'city': self.city,
                                'countryCode': self.country, 'postalCode': self.s_zip,
                                'firstName': self.firstName, 'lastName': self.lastName,
                                'zoneCode': self.state, 'phone': self.phone
                            }
                        },
                        'selectedDeliveryStrategy': {
                            'deliveryStrategyByHandle': {'handle': self.delivery_strategy, 'customDeliveryRate': False},
                            'options': {'phone': self.phone}
                        },
                        'targetMerchandiseLines': {'lines': [{'stableId': self.stableId}]},
                        'deliveryMethodTypes': ['SHIPPING'],
                        'expectedTotalPrice': {'value': {'amount': str(self.shipping_amount), 'currencyCode': self.currency}},
                        'destinationChanged': False
                    }],
                    'noDeliveryRequired': [], 'useProgressiveRates': True, 'prefetchShippingRatesStrategy': None, 'supportsSplitShipping': True
                },
                'merchandise': {
                    'merchandiseLines': [{
                        'stableId': self.stableId,
                        'merchandise': {
                            'productVariantReference': {
                                'id': f'gid://shopify/ProductVariantMerchandise/{self.merch}',
                                'variantId': f'gid://shopify/ProductVariant/{self.variant_id}',
                                'properties': [], 'sellingPlanId': None, 'sellingPlanDigest': None
                            }
                        },
                        'quantity': {'items': {'value': 1}},
                        'expectedTotalPrice': {'value': {'amount': subtotal, 'currencyCode': self.currency}},
                        'lineComponentsSource': None, 'lineComponents': []
                    }]
                },
                'payment': {
                    'totalAmount': {'any': True},
                    'paymentLines': [{
                        'paymentMethod': {
                            'directPaymentMethod': {
                                'paymentMethodIdentifier': self.payment_identifier,
                                'sessionId': token,
                                'billingAddress': {
                                    'streetAddress': {
                                        'address1': self.street, 'address2': '', 'city': self.city,
                                        'countryCode': self.country, 'postalCode': self.s_zip,
                                        'firstName': self.firstName, 'lastName': self.lastName,
                                        'zoneCode': self.state, 'phone': self.phone
                                    }
                                },
                                'cardSource': None
                            }
                        },
                        'amount': {'value': {'amount': self.running_total, 'currencyCode': self.currency}}, 'dueAt': None
                    }],
                    'billingAddress': {
                        'streetAddress': {
                            'address1': self.street, 'address2': '', 'city': self.city,
                            'countryCode': self.country, 'postalCode': self.s_zip,
                            'firstName': self.firstName, 'lastName': self.lastName,
                            'zoneCode': self.state, 'phone': self.phone
                        }
                    }
                },
                'buyerIdentity': {
                    'customer': {'presentmentCurrency': self.currency, 'countryCode': self.country},
                    'email': self.email, 'emailChanged': False, 'phoneCountryCode': self.country,
                    'marketingConsent': [{'email': {'value': self.email}}],
                    'shopPayOptInPhone': {'number': self.phone, 'countryCode': self.country},
                    'rememberMe': False
                },
                'taxes': {
                    'proposedAllocations': None,
                    'proposedTotalAmount': {'value': {'amount': str(self.tax_amount), 'currencyCode': self.currency}},
                    'proposedTotalIncludedAmount': None, 'proposedMixedStateTotalAmount': None, 'proposedExemptions': []
                },
                'tip': {'tipLines': []}, 'note': {'message': None, 'customAttributes': []},
                'localizationExtension': {'fields': []}, 'nonNegotiableTerms': None, 'optionalDuties': {'buyerRefusesDuties': False}
            },
            'attemptToken': self.attempt_token, 'metafields': [], 'analytics': {'requestUrl': self.checkout_url}
        }
        
        if self.checkpoint_data: sub_vars['input']['checkpointData'] = self.checkpoint_data

        submit_json = {'query': MUTATION_SUBMIT, 'variables': sub_vars, 'operationName': 'SubmitForCompletion'}
        
        resp, text = await make_graphql_request_with_captcha_handling(session, graphql_url, {'operationName': 'SubmitForCompletion'}, self.headers, submit_json, self.proxy)
        
        if not resp or is_captcha_required(text): return False, "CAPTCHA_REQUIRED on submit", ""
        if "Your order total has changed." in text: return False, "Site not supported", ""
        if "The requested payment method is not available." in text: return False, "Payment method not available", ""
        
        try:
            r_json = json.loads(text)
            sub_data = r_json.get('data', {}).get('submitForCompletion', {})
            
            if not sub_data:
                errors = r_json.get('errors', [])
                if errors:
                    for error in errors:
                        if code := error.get('code'): return False, code, ""
                return False, "Empty submit response", ""
                
            rtype = sub_data.get('__typename', '')
            if rtype in ('SubmitSuccess', 'SubmittedForCompletion', 'SubmitAlreadyAccepted'):
                rcpt = sub_data.get('receipt', {})
                if rcpt and rcpt.get('__typename') == 'ProcessedReceipt': return True, "ORDER_PLACED", ""
                rid = rcpt.get('id') if rcpt else None
                if not rid: return False, "SubmitSuccess but no receipt", ""
                return True, "POLL", rid
                
            if rtype == 'SubmitFailed':
                return False, extract_clean_response(sub_data.get('reason', 'Unknown reason')), ""
                
            if rtype == 'SubmitRejected':
                for e in sub_data.get('errors', []):
                    code = e.get('code', '')
                    msg = e.get('localizedMessage') or e.get('nonLocalizedMessage')
                    if code in ('GENERIC_ERROR', 'PAYMENT_FAILED', ''):
                        if msg: return False, msg, ""
                    if code: return False, code, ""
                return False, "Submit Rejected", ""
                
            if rtype == 'Throttled': return False, "Throttled", ""
            
            return False, "No receipt ID", ""
        except Exception as e:
            return False, f"Error parsing submit: {str(e)}", ""

    async def _poll_receipt(self, session, graphql_url, rid):
        poll_json = {'query': QUERY_POLL, 'variables': {'receiptId': rid, 'sessionToken': self.sst}, 'operationName': 'PollForReceipt'}
        
        await asyncio.sleep(3) # Explicit timing matched from sp.py
        
        for _ in range(4):
            resp, final_text = await make_graphql_request_with_captcha_handling(session, graphql_url, {'operationName': 'PollForReceipt'}, self.headers, poll_json, self.proxy)
            
            if not resp or is_captcha_required(final_text): return True, "CARD_DECLINED", self.gateway, self.total_price, self.currency
            
            try:
                rdata = json.loads(final_text).get('data', {}).get('receipt', {})
                if rdata:
                    tname = rdata.get('__typename', '')
                    if tname == 'ProcessedReceipt': return True, "ORDER_PLACED", self.gateway, self.total_price, self.currency
                    if tname == 'FailedReceipt':
                        err = rdata.get('processingError', {})
                        err_type = err.get('__typename', '')
                        if err_type == 'PaymentFailed':
                            code = err.get('code', '')
                            msg = err.get('messageUntranslated', '')
                            if code in ('GENERIC_ERROR', 'PAYMENT_FAILED', '') and msg:
                                return True, msg, self.gateway, self.total_price, self.currency
                            return True, code if code else 'PAYMENT_FAILED', self.gateway, self.total_price, self.currency
                        
                        return True, err.get('code') or err_type or 'UNKNOWN_ERROR', self.gateway, self.total_price, self.currency
                    
                    if tname == 'ActionRequiredReceipt': return True, "OTP_REQUIRED", self.gateway, self.total_price, self.currency
                    
                    if tname in ('ProcessingReceipt', 'WaitingReceipt'):
                        await asyncio.sleep(4)
                        continue
            except: pass
            
            if 'WaitingReceipt' in final_text:
                await asyncio.sleep(4)
            else:
                break
                
        if 'CAPTCHA_REQUIRED' in final_text: return True, "CARD_DECLINED", self.gateway, self.total_price, self.currency
        if 'WaitingReceipt' in final_text: return False, "Change Proxy or Site", self.gateway, self.total_price, self.currency
        
        try:
            res_json = json.loads(final_text)
            err_code = res_json.get('data', {}).get('receipt', {}).get('processingError', {}).get('code')
            if "shopify_payments" in str(res_json): return True, "ORDER_PLACED", self.gateway, self.total_price, self.currency
            if err_code: return True, err_code, self.gateway, self.total_price, self.currency
            return True, "MISMATCHED_BILL", self.gateway, self.total_price, self.currency
        except: pass
        
        code = extract_between(final_text, '{"code":"', '"')
        lower_txt = final_text.lower()
        if 'actionreq' in lower_txt or 'action_required' in lower_txt: return True, "OTP_REQUIRED", self.gateway, self.total_price, self.currency
        if 'processedreceipt' in lower_txt: return True, "ORDER_PLACED", self.gateway, self.total_price, self.currency
        if 'failedreceipt' in lower_txt or 'declined' in lower_txt: return True, code if code else "CARD_DECLINED", self.gateway, self.total_price, self.currency
        return False, "Unknown Result", self.gateway, self.total_price, self.currency

def parse_cc_string(cc_string: str) -> dict:
    parts = cc_string.split('|')
    if len(parts) != 4: raise ValueError("Invalid CC format. Use: CC|MM|YYYY|CVV")
    return {'cc': parts[0].strip(), 'mes': parts[1].strip(), 'ano': parts[2].strip(), 'cvv': parts[3].strip()}

# --- API SERVER ---
app = FastAPI(title="Optimized Checkout API")

@app.on_event("startup")
async def startup_event():
    HTTPClient.get_connector()
    logger.info("Connection pool initialized. SSL enforcement DISABLED for proxy compatibility.")

@app.on_event("shutdown")
async def shutdown_event():
    if HTTPClient.connector:
        await HTTPClient.connector.close()
        logger.info("Connection pool closed cleanly.")

@app.get("/shopify")
async def shopify_checker(site: str, cc: str, proxy: Optional[str] = None, variant: Optional[str] = None, retries: int = Query(3, le=5)):
    if not site or not cc:
        raise HTTPException(status_code=400, detail="Missing required parameters 'site' and 'cc'")
        
    try:
        cc_parts = parse_cc_string(cc)
    except ValueError as e:
        return JSONResponse(status_code=400, content={"error": str(e), "status": False})

    sites_to_try = [site]
    
    success, message, gateway, price, currency = False, "No sites found", "UNKNOWN", "0.00", "USD"
    
    conn = HTTPClient.get_connector()
    timeout = aiohttp.ClientTimeout(total=40, connect=5, sock_read=15)
    
    for attempt, current_site in enumerate(sites_to_try):
        if attempt >= retries: break
        
        chk = CheckoutSession(cc_parts, current_site, variant, proxy)
        
        try:
            async with aiohttp.ClientSession(connector=conn, connector_owner=False, timeout=timeout) as session:
                success, message, gateway, price, currency = await chk.run(session)
            
            if success or (not success and "No products under $10 found" not in str(message) and "Out Of Stock" not in str(message)):
                break
        except Exception as e:
            logger.error(f"Execution Error on {current_site}: {str(e)}", exc_info=True)
            message = str(e)
            continue
            
    clean_resp = extract_clean_response(message)
    try:
        p_float = float(price) if price else 0.0
    except:
        p_float = 0.0

    return {
        "Gateway": gateway,
        "Price": p_float,
        "Response": clean_resp,
        "Status": success,
        "cc": cc,
        "PriceUnder3": p_float < 3.00,
        "Attempts": attempt + 1
    }

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"Starting Highly Optimized ASGI Server on port {port}")
    uvicorn.run("api:app", host='0.0.0.0', port=port, log_level="info", access_log=False)