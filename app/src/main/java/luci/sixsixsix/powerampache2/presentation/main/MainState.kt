/**
 * Copyright (C) 2024  Antonio Tari
 *
 * This file is a part of Power Ampache 2
 * Ampache Android client application
 * @author Antonio Tari
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
 *
 */
package luci.sixsixsix.powerampache2.presentation.main

import android.os.Parcelable
import kotlinx.parcelize.Parcelize
import luci.sixsixsix.powerampache2.domain.models.Song

@Parcelize
data class MainState (
    val searchQuery: String = "",
    val errorMessage: String = "",
    val song: Song? = null,
    val queue: List<Song> = listOf(),
    val isLikeLoading:Boolean = false,
    val isFabLoading:Boolean = false,
    val isDownloading:Boolean = false
): Parcelable
