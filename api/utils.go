//
// This file is part of Tiny Queue Service.
//
// Tiny Queue Service is free software: you can redistribute it and/or
// modify it under the terms of the GNU General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// Tiny Queue Service is distributed in the hope that it will be
// useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
//

package api

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/st3fan/tqsd/tqs"
)

func getIntParameter(r *http.Request, name string, def int) (int, error) {
	values, ok := r.URL.Query()[name]
	if !ok {
		return def, nil
	}
	if len(values) != 1 {
		return 0, fmt.Errorf("Expected one <%s> parameter", name)
	}
	return strconv.Atoi(values[0])
}

func getMaxNumberOfMessages(r *http.Request) (int, error) {
	if v, err := getIntParameter(r, "MaxNumberOfMessages", tqs.DefaultMaxNumberOfMessages); err == nil {
		if v >= tqs.MinMaxNumberOfMessages && v <= tqs.MaxMaxNumberOfMessages {
			return v, nil
		}
	}
	return 0, fmt.Errorf("Invalid MaxNumberOfMessages parameter")
}

func getLeaseDuration(r *http.Request) (int, error) {
	if v, err := getIntParameter(r, "LeaseDuration", 0); err == nil { // TODO 0 or -1?
		if v >= tqs.MinLeaseDuration && v <= tqs.MaxLeaseDuration {
			return v, nil
		}
	}
	return 0, fmt.Errorf("Invalid LeaseDuration parameter")
}
