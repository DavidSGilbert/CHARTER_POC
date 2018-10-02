import logging
from nfdk.model_v2.simple_model_pojo_pool import SimpleModelPojoPool
from nfdk.pojos.model import InventoryItemTypeEnum
from nfdk.utils.brain_rest_connector import BrainRestConnector
from nfdk.plugin.plugin_base import PluginBase, PluginType
from nfdk.plugin.output_formaters import textual_output, tabular_output
from itertools import chain
from operator import itemgetter
import asyncio


class CharterInventory(PluginBase):
    GUID = "CHARTERINVENTORY"
    NAME = "Charter POC Inventory Report"
    VERSION = "1.03"
    PLUGIN_TYPE = PluginType.REPORT
    DESCRIPTION = "Charter POC Inventory Report"
    PARAMS_SCHEMA = {}
    TABULAR_HEADERS = ['Type', 'Name', 'Serial Number', 'Description', 'Site Name', 'Site Latitude', 'Site Longitude', 'Parent Node']

    def __init__(self, brain_opts, params):
        self._logger = logging.getLogger(self.GUID)
        self.pool = SimpleModelPojoPool.create_with_rest_connector_from_config_opts(brain_opts,
                                                                                    fail_on_missing_pojo=False)
        self.brain_rest_connector = BrainRestConnector.create_from_config_opts(brain_opts)

    async def run(self, loop):
        def get_fans(one):
            return [fan for fan in fans if fan.parent_non_throwing.guid == one]

        def get_supplies(one):
            """

            :param one:
            :return:
            """
            return [supply for supply in supplies if supply.parent_non_throwing.guid == one]

        def get_shelves(one):
            return [shelf for shelf in shelves if shelf.parent_non_throwing.guid == one]

        def get_cards(shelf):
            return [card for card in cards if card.parent_non_throwing.guid == shelf]

        def get_ports(card):
            return [port for port in ports if port.parent_non_throwing.guid == card]

        def parse_cards(cards):
            temp = []
            for c in cards:
                idx = c.name.split('-')
                temp.append([int(idx[1]), c.guid, c.name, c.serial_number, c.desc, c.type.name])
            return sorted(temp, key=itemgetter(0))

        def parse_ports(ports):
            temp = []
            for p in ports:
                idx = p.name.split('-')
                temp.append([int(idx[2]), p.guid, p.name, p.serial_number, p.desc, p.type.name])
            return sorted(temp, key=itemgetter(0))

        await asyncio.gather(
            self.pool.connector.fetch_inventory(InventoryItemTypeEnum.ONE),
            self.pool.connector.fetch_inventory(InventoryItemTypeEnum.POWER_SUPPLY),
            self.pool.connector.fetch_inventory(InventoryItemTypeEnum.FAN),
            self.pool.connector.fetch_inventory(InventoryItemTypeEnum.SHELF),
            self.pool.connector.fetch_inventory(InventoryItemTypeEnum.CARD),
            self.pool.connector.fetch_inventory(InventoryItemTypeEnum.PORT),
            self.pool.connector.fetch_sites()
        )
        self.pool.resolve_refs()

        ones = [one for one in chain(*[await self.brain_rest_connector.get_inventory(InventoryItemTypeEnum.ONE)])]
        supplies = [supply for supply in chain(*[await self.brain_rest_connector.get_inventory(InventoryItemTypeEnum.POWER_SUPPLY)])]
        fans = [fan for fan in chain(*[await self.brain_rest_connector.get_inventory(InventoryItemTypeEnum.FAN)])]
        shelves = [shelf for shelf in chain(*[await self.brain_rest_connector.get_inventory(InventoryItemTypeEnum.SHELF)])]
        cards = [card for card in chain(*[await self.brain_rest_connector.get_inventory(InventoryItemTypeEnum.CARD)])]
        ports = [port for port in chain(*[await self.brain_rest_connector.get_inventory(InventoryItemTypeEnum.PORT)])]

        rows = []
        for o in ones:
            try:
                one_site = self.pool.get_by_guid(o.site_non_throwing.guid)
                site_info = [one_site.name, one_site.latitude, one_site.longitude, o.name]
            except AttributeError:
                site_info = ['No Site Assigned', '', '']
            rows.append([o.device_type, o.name, '', o.desc] + site_info)
            _shelves = get_shelves(o.guid)
            for s in _shelves:
                rows.append([s.type.name, s.name, s.serial_number, s.desc] + site_info)
                _cards = get_cards(s.guid)
                lo_cards = parse_cards(_cards)
                for c in lo_cards:
                    rows.append([c[5], c[2], c[3], c[4]] + site_info)
                    _ports = get_ports(c[1])
                    lo_ports = parse_ports(_ports)
                    for p in lo_ports:
                        rows.append([p[5], p[2], p[3], p[4]] + site_info)
                _supplies = get_supplies(s.guid)
                for ps in _supplies:
                    rows.append([ps.type.name, ps.name, ps.serial_number, ps.desc] + site_info)
                _fans = get_fans(s.guid)
                for f in _fans:
                    rows.append([f.type.name, f.name, f.serial_number, f.desc] + site_info)

        if rows:
            return tabular_output(headers=self.TABULAR_HEADERS, data=rows)
        else:
            return textual_output("There is no {} for report {}".format('data', self.NAME))

    async def cleanup(self):
        await self.pool.connector.close()
        await self.brain_rest_connector.close_connection()


if __name__ == '__main__':
    CharterInventory.cli()
