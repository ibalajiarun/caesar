/*  
 * Paxos STM - Distributed Software Transactional Memory framework for Java.
 * Copyright (C) 2009-2010  Tadeusz Kobus, Maciej Kokocinski. All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package hyflow.common;

/**
 * Simple generic class representing a pair of values.
 *
 * @param <F> type of the first element of the pair
 * @param <S> type of the second element of the pair
 * @author Tadeusz Kobus
 * @author Maciej Kokocinski
 */
public class Pair<F, S> {
    public F first;
    public S second;

    /**
     * Constructor.
     *
     * @param first  first element of the pair
     * @param second second element of the pair
     */
    public Pair(F first, S second) {
        this.first = first;
        this.second = second;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (!(obj instanceof Pair))
            return false;
        Pair other = (Pair) obj;
        return ((first == null && other.first == null) || first.equals(other.first))
                && ((second == null && other.second == null) || second.equals(other.second));
    }
}
