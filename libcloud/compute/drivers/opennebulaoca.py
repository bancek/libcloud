# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
'''
OpenNebula OCA driver
'''

import urlparse
import oca

from libcloud.common.base import ConnectionUserAndKey
from libcloud.compute.providers import Provider
from libcloud.compute.types import NodeState
from libcloud.compute.base import (NodeDriver, Node, NodeLocation, NodeImage,
                                   NodeSize)

API_HOST = '127.0.0.1'
API_PORT = 2633

class OpenNebulaOCAConnection(ConnectionUserAndKey):
    '''
    Connection class for the OpenNebula driver
    '''
    
    host = API_HOST
    port = API_PORT
    secure = False
    
    def __init__(self, user_id, key, secure=False, host=None, port=None):
        self.user_id = user_id
        self.key = key
        self.host = host
        self.port = port
    
    def connect(self, host=None, port=None):
        host = host or self.host
        port = port or self.port
        
        if not ':' in host:
            host = '%s:%d' % (host, int(port))
        
        auth = ':'.join((self.user_id, self.key))
        url = urlparse.urlunsplit(('http', host, '/RPC2', None, None))
        
        self.client = oca.Client(auth, url)

class OpenNebulaOCANetwork(object):
    def __init__(self, id, name, driver, extra=None):
        self.id = str(id)
        self.name = name
        self.driver = driver
        self.extra = extra or {}

    def __repr__(self):
        return (('<OpenNebulaNetwork: id=%s, name=%s, driver=%s  ...>')
                % (self.id, self.name, self.driver.name))

class OpenNebulaOCANodeDriver(NodeDriver):
    '''
    OpenNebula node driver
    '''

    connectionCls = OpenNebulaOCAConnection
    type = Provider.OPENNEBULA
    name = 'OpenNebula'
    default_cpu = 1
    
    NODE_STATE_LCM_INIT = 0
    NODE_STATE_PROLOG = 1
    NODE_STATE_BOOT = 2
    NODE_STATE_RUNNING = 3
    NODE_STATE_MIGRATE = 4
    NODE_STATE_SAVE_STOP = 5
    NODE_STATE_SAVE_SUSPEND = 6
    NODE_STATE_SAVE_MIGRATE = 7
    NODE_STATE_PROLOG_MIGRATE = 8
    NODE_STATE_PROLOG_RESUME = 9
    NODE_STATE_EPILOG_STOP = 10
    NODE_STATE_EPILOG = 11
    NODE_STATE_SHUTDOWN = 12
    NODE_STATE_CANCEL = 13
    NODE_STATE_FAILURE = 14
    NODE_STATE_DELETE = 15
    NODE_STATE_UNKNOWN = 16
    
    NODE_STATE_MAP = {
        NODE_STATE_LCM_INIT: NodeState.PENDING,
        NODE_STATE_PROLOG: NodeState.PENDING,
        NODE_STATE_BOOT: NodeState.PENDING,
        NODE_STATE_RUNNING: NodeState.RUNNING,
        NODE_STATE_MIGRATE: NodeState.TERMINATED,
        NODE_STATE_SAVE_STOP: NodeState.TERMINATED,
        NODE_STATE_SAVE_SUSPEND: NodeState.TERMINATED,
        NODE_STATE_SAVE_MIGRATE: NodeState.TERMINATED,
        NODE_STATE_PROLOG_MIGRATE: NodeState.TERMINATED,
        NODE_STATE_PROLOG_RESUME: NodeState.TERMINATED,
        NODE_STATE_EPILOG_STOP: NodeState.TERMINATED,
        NODE_STATE_EPILOG: NodeState.TERMINATED,
        NODE_STATE_SHUTDOWN: NodeState.TERMINATED,
        NODE_STATE_CANCEL: NodeState.TERMINATED,
        NODE_STATE_FAILURE: NodeState.UNKNOWN,
        NODE_STATE_DELETE: NodeState.TERMINATED,
        NODE_STATE_UNKNOWN: NodeState.UNKNOWN,
    }
    
    def list_sizes(self, location=None):
        return [
            NodeSize(id=1,
                     name='default',
                     ram=512,
                     disk=None,
                     bandwidth=None,
                     price=None,
                     driver=self),
        ]
    
    def _get_vms(self):
        pool = oca.VirtualMachinePool(self.connection.client)
        pool.info(-2)
        return pool
    
    def _get_images(self):
        pool = oca.ImagePool(self.connection.client)
        pool.info(-2)
        return pool
    
    def _get_vns(self):
        pool = oca.VirtualNetworkPool(self.connection.client)
        pool.info(-2)
        return pool
    
    def list_nodes(self):
        return self._to_nodes(self._get_vms())

    def list_images(self, location=None):
        return self._to_images(self._get_images())

    def ex_list_networks(self):
        return self._to_networks(self._get_vns())

    def list_locations(self):
        return [NodeLocation(0,  'OpenNebula', 'ONE', self)]

    def reboot_node(self, node):
        vms = self._get_vms()
        vm = vms.get_by_id(int(node.id))
        vm.restart()
        
        return True

    def destroy_node(self, node):
        vms = self._get_vms()
        vm = vms.get_by_id(int(node.id))
        vm.finalize()
        
        return True
    
    def get_template(self, name, cpu, size, image, network,
                     context=None, template_factory=None):
        
        template = (template_factory or (lambda: {}))()
        
        template.setdefault('os', {})
        template['os'].setdefault('arch', 'i686')
        template['os'].setdefault('boot', 'hd')
        template['os'].setdefault('root', 'hda')
        
        template.setdefault('disk', {})
        template['disk'].setdefault('bus', 'scsi')
        template['disk'].setdefault('readonly', False)
        
        template.setdefault('nic', {})
        
        template.setdefault('graphics', {})
        template['graphics'].setdefault('type', 'vnc')
        
        template.setdefault('context', {})
        
        template['name'] = name
        template['cpu'] = cpu
        template['memory'] = size.ram
        template['disk']['image_id'] = image.id
        template['nic']['network_id'] = network.id
        
        template['context'].update(context or {})
        
        template['context'] = template['context'] or None
        
        return template
    
    def create_node(self, **kwargs):
        '''Create a new OpenNebula node

        See L{NodeDriver.create_node} for more keyword args.
        '''
        
        name = kwargs['name']
        cpu = float(kwargs.get('cpu', self.default_cpu))
        size = kwargs['size']
        image = kwargs['image']
        network = kwargs['network']
        context = kwargs.get('context', None)
        template_factory = kwargs.get('template_factory', None)
        
        template = self.get_template(name, cpu, size, image, network,
                                     context, template_factory)
        
        tpl = str(OneTemplate(template))
        
        vm_id = oca.VirtualMachine.allocate(self.connection.client, tpl)
        
        vms = self._get_vms()
        vm = vms.get_by_id(int(vm_id))
        
        return self._to_node(vm)
    
    def _to_images(self, oca_images):
        images = map(self._to_image, oca_images)
        return images

    def _to_image(self, oca_image):
        return NodeImage(id=oca_image.id,
                         name=oca_image.name,
                         driver=self.connection.driver,
                         extra=vars(oca_image))
    
    def _to_networks(self, oca_vns):
        networks = map(self._to_network, oca_vns)
        return networks

    def _to_network(self, oca_vn):
        return OpenNebulaOCANetwork(id=oca_vn.id,
                                    name=oca_vn.name,
                                    driver=self.connection.driver,
                                    extra=vars(oca_vn))
    
    def _to_nodes(self, oca_vms):
        computes = map(self._to_node, oca_vms)
        return computes

    def _to_node(self, oca_vm):
        state = self.NODE_STATE_MAP.get(oca_vm.lcm_state, NodeState.UNKNOWN)
        networks = [x.ip for x in oca_vm.template.nics]
        
        return Node(id=oca_vm.id,
                    name=oca_vm.name,
                    state=state,
                    public_ip=networks[0],
                    private_ip=[],
                    driver=self.connection.driver,
                    extra=vars(oca_vm))

class OneTemplate(object):
    INDENT = ' ' * 4
    
    def __init__(self, template):
        self.template = template
    
    def _wrap(self, txt, char='"'):
        return char + txt.replace(char, '\\' + char) + char
    
    def serialize(self, node, level=0):
        if node is None:
            yield node
        elif isinstance(node, dict):
            if level:
                yield '['
            
            rows = []
            
            for key, value in node.items():
                serialized = list(self.serialize(value, level + 1))
                
                if serialized != [None]:
                    rows.append(self.INDENT * level + '%s = %s' % (
                                                key, '\n'.join(serialized)))
            
            rows_count = len(rows)
            
            for i, row in enumerate(rows):
                if level and i < rows_count - 1:
                    yield row + ','
                else:
                    yield row
            
            if level:
                yield self.INDENT * (level - 1) + ']'
        elif isinstance(node, bool):
            yield self._wrap('yes') if node else self._wrap('no')
        elif isinstance(node, basestring):
            yield self._wrap(node)
        else:
            yield str(node)
    
    def __str__(self):
        return '\n'.join(self.serialize(self.template))
