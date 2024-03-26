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
package luci.sixsixsix.powerampache2.data

import androidx.lifecycle.LiveData
import androidx.lifecycle.distinctUntilChanged
import androidx.lifecycle.map
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import luci.sixsixsix.mrlog.L
import luci.sixsixsix.powerampache2.BuildConfig
import luci.sixsixsix.powerampache2.common.Constants
import luci.sixsixsix.powerampache2.common.Resource
import luci.sixsixsix.powerampache2.common.sha256
import luci.sixsixsix.powerampache2.data.local.MusicDatabase
import luci.sixsixsix.powerampache2.data.local.entities.CredentialsEntity
import luci.sixsixsix.powerampache2.data.local.entities.toGenre
import luci.sixsixsix.powerampache2.data.local.entities.toGenreEntity
import luci.sixsixsix.powerampache2.data.local.entities.toSession
import luci.sixsixsix.powerampache2.data.local.entities.toSessionEntity
import luci.sixsixsix.powerampache2.data.local.entities.toUser
import luci.sixsixsix.powerampache2.data.local.entities.toUserEntity
import luci.sixsixsix.powerampache2.data.remote.MainNetwork
import luci.sixsixsix.powerampache2.data.remote.dto.toBoolean
import luci.sixsixsix.powerampache2.data.remote.dto.toError
import luci.sixsixsix.powerampache2.data.remote.dto.toGenre
import luci.sixsixsix.powerampache2.data.remote.dto.toServerInfo
import luci.sixsixsix.powerampache2.data.remote.dto.toSession
import luci.sixsixsix.powerampache2.data.remote.dto.toUser
import luci.sixsixsix.powerampache2.domain.MusicRepository
import luci.sixsixsix.powerampache2.domain.errors.ErrorHandler
import luci.sixsixsix.powerampache2.domain.errors.MusicException
import luci.sixsixsix.powerampache2.domain.mappers.DateMapper
import luci.sixsixsix.powerampache2.domain.models.ServerInfo
import luci.sixsixsix.powerampache2.domain.models.Session
import luci.sixsixsix.powerampache2.domain.models.User
import retrofit2.HttpException
import java.io.IOException
import java.time.Instant
import javax.inject.Inject
import javax.inject.Singleton

/**
 * the source of truth is the database, stick to the single source of truth pattern, only return
 * data from database, when making a network call first insert data into db then read from db and
 * return/emit data.
 * When breaking a rule please add a comment with a TODO: BREAKING_RULE
 */
@OptIn(DelicateCoroutinesApi::class)
@Singleton
class MusicRepositoryImpl @Inject constructor(
    private val api: MainNetwork,
    private val dateMapper: DateMapper,
    db: MusicDatabase,
    private val errorHandler: ErrorHandler
): BaseAmpacheRepository(api, db, errorHandler), MusicRepository {
    private val _serverInfoStateFlow = MutableStateFlow(ServerInfo())
    override val serverInfoStateFlow: StateFlow<ServerInfo> = _serverInfoStateFlow
    override val sessionLiveData = dao.getSessionLiveData().map { it?.toSession() }
    override val userLiveData: LiveData<User?>
        get() = userLiveData()

    // used to check if a call to getUserNetwork() is necessary
    private var currentAuthToken: String? = null
    private var currentUser: User? = null
    init {
        // Things to do when we get new or different session
        // user will itself emit a user object to observe
        sessionLiveData.distinctUntilChanged().observeForever { session ->
            session?.auth?.let {
                // if token has changed or user is null, get user from network
                if (it != currentAuthToken || currentUser == null)
                    currentAuthToken = it
                    GlobalScope.launch {
                        try {
                            getUserNetwork()
                        } catch (e: Exception) {
                            errorHandler.logError(e)
                        }
                    }
            }
        }
    }

    private fun userLiveData() = dao.getUserLiveData().map { it?.toUser() }

    private suspend fun setSession(se: Session) {
        if (se.auth != getSession()?.auth) {
            // albums, songs, playlists and artist have links that contain the auth token
            //dao.clearCachedData()
            //dao.clearPlaylists()
            L("setSession se.auth != getSession()?.auth")
        }
        dao.updateSession(se.toSessionEntity())
    }

    private suspend fun getUserNetwork() {
        getCredentials()?.username?.let { username ->
            getSession()?.let { session ->
                api.getUser(authKey = session.auth, username = username)
                    .also { userDto ->
                        userDto.id?.let {
                            userDto.toUser().let { us ->
                                currentUser = us
                                setUser(us)
                            }
                        }
                    }
            }
        }
    }

    /**
     * updating the user in the database will trigger the user live data, observe that
     * to get updates on the user
     */
    private suspend fun setUser(user: User) =
        dao.updateUser(user.toUserEntity())

    private suspend fun setCredentials(se: CredentialsEntity) =
        dao.updateCredentials(se)

    override suspend fun logout(): Flow<Resource<Boolean>> = flow {
        emit(Resource.Loading(true))
        val currentAuth = getSession()?.auth // need this to make the logout call
        val resp = currentAuth?.let {
            api.goodbye(it)
        }
        dao.clearCredentials()
        dao.clearSession()
        dao.clearCachedData()
        dao.clearUser()

        L( "LOGOUT $resp")

        if (resp?.toBoolean() == true) {
            emit(Resource.Success(true))
        } else {
            // do not show anything to the user if in prod mode, log error instead
            errorHandler.logError("there is an error in the logout response.\nLOGOUT $resp")
            throw Exception(if (BuildConfig.DEBUG) "there is an error in the logout response" else "")
        }

        emit(Resource.Loading(false))
    }.catch { e -> errorHandler("logout()", e, this) }

    override suspend fun ping(): Resource<Pair<ServerInfo, Session?>> =
        try {
            val dbSession = getSession()
            val pingResponse = api.ping(dbSession?.auth ?: "")

            // Updated session only valid of previous session exists, authorize otherwise
            dbSession?.let { cachedSession ->
                try {
                    // add credentials to the new session
                    pingResponse.toSession(dateMapper)
                    // TODO Check connection error before making this call crash into the try-catch
                } catch (e: Exception) {
                    L("clear session, set the session to null")
                    dao.clearSession()
                    null
                } ?.let { se ->
                    if (se.auth != null) {
                        // save new session if auth is not null
                        setSession(se)
                    }
                }
            }

            // server info always available
            val servInfo = pingResponse.toServerInfo()
            L("aaa setting live data for server info $servInfo")
            _serverInfoStateFlow.value = servInfo
            Resource.Success(Pair(servInfo, getSession()))
        } catch (e: IOException) {
            Resource.Error(message = "cannot load data", exception = e)
        } catch (e: HttpException) {
            Resource.Error(message = "cannot load data", exception = e)
        } catch (e: MusicException) {
            Resource.Error(message = e.musicError.toString(), exception = e)
        } catch (e: Exception) {
            Resource.Error(message = "cannot load data", exception = e)
        }

    suspend fun autoLoginOld(): Flow<Resource<Session>> {
        val credentials = getCredentials()
        // authorization with empty string will fail
        return authorize(
            credentials?.username ?: "",
            credentials?.password ?: "",
            credentials?.serverUrl ?: "",
            credentials?.authToken ?: "",
            true
        )
    }

    override suspend fun autoLogin() = getCredentials()?.let {
        authorize(
            it.username,
            it.password,
            it.serverUrl,
            it.authToken,
            true
        )
    } ?: authorize("", "", "", "", true)


    override suspend fun authorize(
        username: String,
        sha256password: String,
        serverUrl: String,
        authToken: String,
        force: Boolean
    ): Flow<Resource<Session>> = flow {
        emit(Resource.Loading(true))
        //   Save current credentials, so they can be picked up by the interceptor,
        // and for future autologin, this has to be first line of code before any network call
        setCredentials(CredentialsEntity(username = username, password = sha256password, serverUrl = serverUrl, authToken = authToken))
        L("authorize CREDENTIALS ${getCredentials()}")
        val auth = tryAuthorize(username, sha256password, authToken, force)
        emit(Resource.Success(auth))
        emit(Resource.Loading(false))
    }.catch { e -> errorHandler("authorize()", e, this) }

    @Throws(Exception::class)
    private suspend fun tryAuthorize(
        username:String,
        sha256password:String,
        authToken: String,
        force: Boolean,
    ): Session {
        val session = getSession()
        if (session == null || session.isTokenExpired() || force) {
            val auth = if (authToken.isBlank()) {
                    val timestamp = Instant.now().epochSecond
                    val authHash = "$timestamp$sha256password".sha256()
                    api.authorize(authHash = authHash, user = username, timestamp = timestamp)
                } else {
                    api.authorize(apiKey = authToken)
                }
            auth.error?.let { throw (MusicException(it.toError())) }
            auth.auth?.let {
                L("NEW auth $auth")
                auth.toSession(dateMapper).also { sess ->
                    setSession(sess)
                    L("auth token was null or expired", sess.sessionExpire,
                        "\nisTokenExpired?", sess.isTokenExpired(),
                        "new auth", sess.auth)
                }
            }
        }
        return getSession()!! // will throw exception if session null
    }

    override suspend fun register(
        serverUrl: String,
        username: String,
        sha256password: String,
        email: String,
        fullName: String?
    ): Flow<Resource<Any>> = flow {
        emit(Resource.Loading(true))

        setCredentials(CredentialsEntity(
            username = username,
            password = sha256password,
            serverUrl = serverUrl,
            authToken = "")
        )

        val resp = api.register(
            username = username,
            password = sha256password,
            email = email,
            fullName = fullName
        )

        resp.error?.let { throw (MusicException(it.toError())) }
        resp.success?.let {
            emit(Resource.Success(it))
        } ?: run {
            // do not show anything to the user if in prod mode, log error instead
            errorHandler.logError("there is an error in the logout response.\nLOGOUT $resp")
            throw Exception(if (BuildConfig.DEBUG) "there is an error registering your account\nIs user registration allowed on the server?" else "")
        }

        emit(Resource.Loading(false))
    }.catch { e -> errorHandler("register()", e, this) }

    override suspend fun getGenres(fetchRemote: Boolean) = flow {
        emit(Resource.Loading(true))

        val localGenres = dao.getGenres()
        val isDbEmpty = localGenres.isEmpty()
        if (!isDbEmpty) {
            emit(Resource.Success(data = localGenres.map { it.toGenre() }))
        }
        val shouldLoadCacheOnly = !isDbEmpty && !fetchRemote
        if(shouldLoadCacheOnly) {
            emit(Resource.Loading(false))
            return@flow
        }

        val auth = getSession()!!
        val response = api.getGenres(authKey = auth.auth).genres!!.map { it.toGenre() }


        if (Constants.CLEAR_TABLE_AFTER_FETCH) {
            dao.clearGenres()
        }
        dao.insertGenres(response.map { it.toGenreEntity() })
        // stick to the single source of truth pattern despite performance deterioration
        emit(Resource.Success(data = dao.getGenres().map { it.toGenre() }, networkData = response))

        emit(Resource.Loading(false))
    }.catch { e -> errorHandler("getGenres()", e, this) }
}
