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
package luci.sixsixsix.powerampache2.domain

import androidx.lifecycle.LiveData
import kotlinx.coroutines.flow.Flow
import luci.sixsixsix.powerampache2.common.Resource
import luci.sixsixsix.powerampache2.data.remote.dto.SuccessResponse
import luci.sixsixsix.powerampache2.domain.models.ServerInfo
import luci.sixsixsix.powerampache2.domain.models.Session
import luci.sixsixsix.powerampache2.domain.models.User
import retrofit2.http.Query

interface MusicRepository {

    val sessionLiveData: LiveData<Session?>
    val userLiveData: LiveData<User?>
    suspend fun ping(): Resource<Pair<ServerInfo, Session?>>
    suspend fun autoLogin(): Flow<Resource<Session>>
    suspend fun logout(): Flow<Resource<Boolean>>
    suspend fun authorize(username:String, password:String, serverUrl: String, authToken: String, force: Boolean = true): Flow<Resource<Session>>
    suspend fun getUser(): User?
    suspend fun register(serverUrl: String, username: String, password: String, email: String, fullName: String? = null): Flow<Resource<Any>>
}
