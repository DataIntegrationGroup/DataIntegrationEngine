# ===============================================================================
# Copyright 2024 Jake Ross
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ===============================================================================


class Config:
    bbox = None
    output_path = "output"
    use_csv = True
    use_geojson = False
    use_source_ampapi = True
    use_source_isc_seven_rivers = True

    def bounding_wkt(self):
        p1, p2 = self.bbox.split(',')
        x1,y1 = [float(a) for a in p1.strip().split(' ')]
        x2,y2 = [float(a) for a in p2.strip().split(' ')]

        pts = [f'{x1} {y1}',
               f'{x1} {y2}',
               f'{x2} {y2}',
               f'{x2} {y1}',
               f'{x1} {y1}']

        pts = ','.join(pts)
        return f'POLYGON(({pts}))'

# ============= EOF =============================================
