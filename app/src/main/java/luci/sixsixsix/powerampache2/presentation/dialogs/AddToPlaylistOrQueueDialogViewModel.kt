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
package luci.sixsixsix.powerampache2.presentation.dialogs

import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import dagger.hilt.android.lifecycle.HiltViewModel
import kotlinx.coroutines.launch
import luci.sixsixsix.mrlog.L
import luci.sixsixsix.powerampache2.common.Resource
import luci.sixsixsix.powerampache2.data.remote.MainNetwork
import luci.sixsixsix.powerampache2.domain.MusicRepository
import luci.sixsixsix.powerampache2.domain.PlaylistsRepository
import luci.sixsixsix.powerampache2.domain.models.Playlist
import luci.sixsixsix.powerampache2.domain.models.Song
import luci.sixsixsix.powerampache2.player.MusicPlaylistManager
import java.util.UUID
import javax.inject.Inject

@HiltViewModel
class AddToPlaylistOrQueueDialogViewModel @Inject constructor(
    private val playlistsRepository: PlaylistsRepository,
    private val musicRepository: MusicRepository,
    private val playlistManager: MusicPlaylistManager
) : ViewModel() {
    var state by mutableStateOf(AddToPlaylistOrQueueDialogState())
    private var isEndOfDataReached: Boolean = false

    init {
        getPlaylists()
        // playlists can change or be edited, make sure to always listen to the latest version
        playlistsRepository.playlistsLiveData.observeForever {
            viewModelScope.launch {
                L("viewmodel.getPlaylists observed playlist change", state.playlists.size)
                val playlists = filterPlaylists(it)
                if (playlists.isNotEmpty() && state.playlists != playlists) {
                    L("viewmodel.getPlaylists playlists are different, update")
                    state = state.copy(playlists = playlists)
                }
            }
        }
    }

    private suspend fun filterPlaylists(playlists: List<Playlist>) =
        ArrayList<Playlist>().apply {
            playlists.forEach { playlist: Playlist ->
                L(musicRepository.getUser()?.username, playlist.owner)
                if (playlist.owner == musicRepository.getUser()?.username) {
                    add(playlist)
                }
            }
        }

    fun onEvent(event: AddToPlaylistOrQueueDialogEvent) {
        when (event) {
            is AddToPlaylistOrQueueDialogEvent.AddSongToPlaylist ->
                addSongToPlaylist(playlistId = event.playlistId, songId = event.song.mediaId)
            is AddToPlaylistOrQueueDialogEvent.CreatePlaylistAndAddSong ->
                createPlaylistAddSong(
                    playlistName = event.playlistName.ifBlank { UUID.randomUUID().toString() },
                    playlistType = event.playlistType,
                    songId = event.song.mediaId
                )
            is AddToPlaylistOrQueueDialogEvent.OnAddAlbumToQueue ->
                playlistManager.addToCurrentQueue(event.songs)
            is AddToPlaylistOrQueueDialogEvent.AddSongsToPlaylist ->
                addSongsToPlaylist(songs = event.songs, playlist =  event.playlist)
            is AddToPlaylistOrQueueDialogEvent.CreatePlaylistAndAddSongs ->
                createPlaylistAndAddSongs(event.playlistName, event.playlistType, event.songs)
        }
    }

    private fun getPlaylists(
        fetchRemote: Boolean = true,
        offset: Int = 0
    ) {
        viewModelScope.launch {
            playlistsRepository
                .getPlaylists(fetchRemote, "", offset)
                .collect { result ->
                    when (result) {
                        is Resource.Success -> {
                            result.data?.let {
                                val playlists = filterPlaylists(it)
                                if (state.playlists != playlists) {
                                    L("viewmodel.getPlaylists playlists are different, update")
                                    state = state.copy(playlists = playlists)
                                }
                                L("viewmodel.getPlaylists size", state.playlists.size)
                            }
                            isEndOfDataReached = (result.networkData?.isEmpty() == true && offset > 0)
                            L("viewmodel.getPlaylists is bottom reached?", isEndOfDataReached, "offset", offset, "size of new array", result.networkData?.size)
                        }
                        is Resource.Error -> {
                            L("ERROR PlaylistsViewModel", result.exception)
                        }
                        is Resource.Loading -> {
                            state = state.copy(isLoading = result.isLoading)
                        }
                    }
                }
        }
    }

    private fun createPlaylistAddSong(
        playlistName: String,
        playlistType: MainNetwork.PlaylistType = MainNetwork.PlaylistType.private,
        songId: String
    ) = viewModelScope.launch {
        playlistsRepository
            .createNewPlaylist(name = playlistName, playlistType = playlistType).collect { result ->
                when (result) {
                    is Resource.Success -> {
                        result.data?.let {
                           L("playlist created, now add songs")
                            addSongToPlaylist(playlistId = it.id, songId = songId)} }
                    is Resource.Error ->
                        state = state.copy(isPlaylistEditLoading = false)
                    is Resource.Loading ->
                        state = state.copy(isPlaylistEditLoading = result.isLoading)
                }
            }
    }

    private fun createPlaylistAndAddSongs(
        playlistName: String,
        playlistType: MainNetwork.PlaylistType = MainNetwork.PlaylistType.private,
        songsToAdd: List<Song>
    ) = viewModelScope.launch {
        playlistsRepository
            .createNewPlaylistAddSongs(playlistName, playlistType, songsToAdd).collect { result ->
                when (result) {
                    is Resource.Success -> {
                        result.data?.let {
                            getPlaylists()
                            playlistManager.updateUserMessage("Playlist $playlistName created and songs added")
                        }
                    }
                    is Resource.Error ->
                        state = state.copy(isPlaylistEditLoading = false)
                    is Resource.Loading ->
                        state = state.copy(isPlaylistEditLoading = result.isLoading)
                }
            }
    }

    private fun addSongToPlaylist(playlistId: String, songId: String) = viewModelScope.launch {
        playlistsRepository
            .addSongToPlaylist(playlistId = playlistId, songId = songId).collect { result ->
                when (result) {
                    is Resource.Success -> {
                        result.data?.let {
                            getPlaylists()
                            playlistManager.updateUserMessage("Song added to playlist")
                        }
                    }
                    is Resource.Error ->
                        state = state.copy(isPlaylistEditLoading = false)
                    is Resource.Loading ->
                        state = state.copy(isPlaylistEditLoading = result.isLoading)
                }
            }
    }

    private fun addSongsToPlaylist(playlist: Playlist, songs: List<Song>) = viewModelScope.launch {
        playlistsRepository
            .addSongsToPlaylist(playlist = playlist, songsToAdd = songs).collect { result ->
                when (result) {
                    is Resource.Success -> {
                        result.data?.let {
                            getPlaylists()
                            playlistManager.updateUserMessage("Songs added to playlist ${playlist.name}")
                        }
                    }
                    is Resource.Error ->
                        state = state.copy(isPlaylistEditLoading = false)
                    is Resource.Loading ->
                        state = state.copy(isPlaylistEditLoading = result.isLoading)
                }
            }
    }
}

data class AddToPlaylistOrQueueDialogState (
    val playlists: List<Playlist> = emptyList(),
    val isLoading: Boolean = false,
    val isPlaylistEditLoading: Boolean = false,
)

sealed class AddToPlaylistOrQueueDialogEvent {
    data class OnAddAlbumToQueue(val songs: List<Song>): AddToPlaylistOrQueueDialogEvent()
    data class AddSongsToPlaylist(val songs: List<Song>, val playlist: Playlist): AddToPlaylistOrQueueDialogEvent()
    data class CreatePlaylistAndAddSongs(val songs: List<Song>, val playlistName: String, val playlistType: MainNetwork.PlaylistType): AddToPlaylistOrQueueDialogEvent()
    data class AddSongToPlaylist(val song: Song, val playlistId: String): AddToPlaylistOrQueueDialogEvent()
    data class CreatePlaylistAndAddSong(val song: Song, val playlistName: String, val playlistType: MainNetwork.PlaylistType): AddToPlaylistOrQueueDialogEvent()
}
