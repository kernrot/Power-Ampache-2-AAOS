/**
 * Copyright (C) 2025  Antonio Tari
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
package luci.sixsixsix.powerampache2.data.offlinemode

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.retryWhen
import luci.sixsixsix.mrlog.L
import luci.sixsixsix.powerampache2.data.local.MusicDatabase
import luci.sixsixsix.powerampache2.data.local.entities.toSong
import luci.sixsixsix.powerampache2.di.OfflineModeDataSource
import luci.sixsixsix.powerampache2.domain.datasource.SongsOfflineDataSource
import luci.sixsixsix.powerampache2.domain.models.Song
import javax.inject.Inject

@OfflineModeDataSource
class SongsOfflineDataSourceImpl @Inject constructor(
    db: MusicDatabase,
): SongsOfflineDataSource {
    private val dao = db.dao

    override val offlineSongsFlow = dao.getCurrentMultiuserIdFlow()
        .flatMapLatest { multiUserId ->
            if (multiUserId == null) emptyFlow()
            else dao.downloadedSongsFromIdFlow(multiUserId)
                .map { entities -> entities.map { it.toSong() } }
                //.distinctUntilChanged()
                //DEBUG: onEach { println("aaaaa emitting downloaded songs ${it.size}") }
                .retryWhen { cause, attempt ->
                    L("aaaa Flow failed, retry attempt $attempt due to ${cause.message}")
                    delay(1000)  // wait before retrying
                    attempt < 11  // retry up to 11 times
                }
                .catch { e -> emit(emptyList()).also { println("aaaaa Caught exception in flow: ${e.message}") } }
    }

    override suspend fun getRecentSongs(): List<Song> =
        dao.getOfflineSongHistory().map { it.toSong() }
}
